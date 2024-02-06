package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"
)

type TConfig struct {
	Conf   *Conf   `json:"conf" yaml:"conf"`
	Input  *Input  `json:"input" yaml:"input"`
	Output *Output `json:"output" yaml:"output"`
	Handle *Handle `json:"handle" yaml:"handle"`
}

type Conf struct {
	Batch *int `json:"batch" yaml:"batch"`
	//Concurrency *int `json:"concurrency" yaml:"concurrency"`
}

type Input struct {
	FilePath  *string `json:"filePath" yaml:"filePath"`
	Delimiter *string `json:"delimiter" yaml:"delimiter"`
	IsHeader  *bool   `json:"isHeader" yaml:"isHeader"`
}

type Output struct {
	FilePath  *string `json:"filePath" yaml:"filePath"`
	FailPath  *string `json:"failPath" yaml:"failPath"`
	Delimiter *string `json:"delimiter" yaml:"delimiter"`
}

type Handle struct {
	Transform []*Field `json:"transform" yaml:"transform"`
	FilterRow []*Field `json:"filterRow" yaml:"filterRow"`
	FilterCol []*Field `json:"filterCol" yaml:"filterCol"`
}

type Field struct {
	Method *string  `json:"method" yaml:"method"`
	Col    *string  `json:"col" yaml:"col"`
	Params []*Param `json:"params" yaml:"params"`
	Value  *string  `json:"value" yaml:"value"`
	Mode   *int     `json:"mode" yaml:"mode"`
	CutCol *string  `json:"cutCol" yaml:"cutCol"`
	AddCol *string  `json:"addCol" yaml:"addCol"`
}

type Param struct {
	Param *string `json:"param" yaml:"param"`
}

type ContextMgr struct {
	TConfig      *TConfig
	HandleSchema []HandleSchema
	//ConcurrencyChs []chan []string
	Statistics    *Statistics
	RwNoticeCh    chan int
	RwActNoticeCh chan int
	RwDoneCh      chan int
	StateCh       chan int
}

type HandleSchema struct {
	Type     string
	Function []Function
}

type Statistics struct {
	FinishLineNum   int
	StageFinishTime int64
	FilterLineNum   int
	StageFilterTime int64
	ErrorLineNum    int
	StageErrorTime  int64
	StartReadTime   int64
}

type Function struct {
	FuncName   string
	FuncColumn int
	FuncParams []string
	FuncMode   int
}

const (
	transform = "transform"
	filterRow = "filterRow"
	filterCol = "filterCol"
)

const (
	ToUpper         = "ToUpper"
	ToLower         = "ToLower"
	Replace         = "Replace"
	TrimSpace       = "TrimSpace"
	JoinLeft        = "JoinLeft"
	JoinRight       = "JoinRight"
	TimeToUnix      = "TimeToUnix"
	TimeToUnixMilli = "TimeToUnixMilli"
	UnixToTime      = "UnixToTime"
	TimeToTime      = "TimeToTime"
)

const (
	GT  = "GT"
	GTE = "GTE"
	LT  = "LT"
	LTE = "LTE"
	EQ  = "EQ"
	NEQ = "NEQ"
)

const (
	CUT         = "Cut"
	FixedValue  = "FixedValue"
	RandomValue = "RandomValue"
	CurrentTime = "CurrentTime"
)

const (
	CommonType = "CommonType"
	ErrorType  = "ErrorType"
)

var (
	defaultBatch       = 1000
	defaultConcurrency = -1
	defaultDelimiter   = ","
	defaultIsHead      = false
	defaultTimeMap     = map[string]string{
		"yyyy-mm-dd hh:mm:ss": "2006-01-02 15:04:05",
		"yyyy/mm/dd hh:mm:ss": "2006/01/02 15:04:05",
		"yyyy-mm-dd":          "2006-01-02",
		"yyyy/mm/dd":          "2006/01/02",
		"hh:mm:ss":            "15:04:05",
	}
)

var configuration = flag.String("config", "", "Specify transform configure file path")

//全局
var wg sync.WaitGroup

func main() {
	log.Printf("Goroutine main process.")
	errCode := 0
	defer func() {
		time.Sleep(1 * time.Second)
		os.Exit(errCode)
	}()

	flag.Parse()

	//解析配置
	conf, err := ConfParse(*configuration)
	if err != nil {
		log.Println(err.Error())
		errCode = -1
		return
	}

	startTime := time.Now()
	log.Printf("Start time: %s.", startTime)

	//初始化读取
	err = InitRead(*conf)
	if err != nil {
		fmt.Println("Init read err", err.Error())
		errCode = -1
		return
	}

	//初始化写出
	err = InitWrite(*conf)
	if err != nil {
		fmt.Println("Init write err", err.Error())
		errCode = -1
		return
	}

	//初始化Context
	contextMgr, err := InitContext(conf)
	if err != nil {
		fmt.Println("Init contextMgr err", err.Error())
		errCode = -1
		return
	}
	contextMgr.Statistics.StartReadTime = startTime.Unix()

	//定义数据channel，用于读取与写出
	dataCh := make(chan string, *contextMgr.TConfig.Conf.Batch*10)
	//定义错误数据channel，用于读取与错误写出
	errDataCh := make(chan string, *contextMgr.TConfig.Conf.Batch*10)

	wg.Add(5)
	//启动一个协程，读取文件
	go ReadFile(contextMgr, dataCh, errDataCh)

	//启动一个协程，写出错误数据文件
	go WriteErrDataFile(contextMgr, errDataCh)

	//启动一个协程，写出文件
	go WriteFile(contextMgr, dataCh)

	//启动一个协程，监控统计
	go PrintStatistics(contextMgr)

	//关闭读写信号传输channel（只能关闭一次）
	go CloseRwChannel(contextMgr)

	wg.Wait()

	endTime := time.Now()
	log.Printf("End time is %s.", endTime)

	costTime := endTime.Unix() - contextMgr.Statistics.StartReadTime
	totalCount := contextMgr.Statistics.FinishLineNum + contextMgr.Statistics.FilterLineNum + contextMgr.Statistics.ErrorLineNum
	if costTime <= 0 {
		costTime = 1
	}
	log.Printf("Total: Cost Time(%ds), Finished(%d), Filter(%d), Failed(%d), Rows AVG(%.2f/s).",
		costTime, contextMgr.Statistics.FinishLineNum, contextMgr.Statistics.FilterLineNum, contextMgr.Statistics.ErrorLineNum, float64(totalCount)/cast.ToFloat64(costTime))
}

func ConfParse(filename string) (*TConfig, error) {
	log.Printf("Parse config and verify config.")
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var conf TConfig
	if err = yaml.Unmarshal(content, &conf); err != nil {
		return nil, err
	}
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return &conf, nil
}

func (config *TConfig) Validate() error {
	if config.Conf == nil {
		config.Conf = &Conf{
			Batch: &defaultBatch,
			//Concurrency: &defaultConcurrency,
		}
		log.Printf("Reset config [conf.batch] value to %d", config.Conf.Batch)
	}
	if err := config.Conf.ValidateAndReset(); err != nil {
		return err
	}
	if config.Input == nil {
		return fmt.Errorf("no find [input] config")
	}
	if err := config.Input.ValidateAndReset(); err != nil {
		return err
	}
	if config.Output == nil {
		return fmt.Errorf("no find [output] config")
	}
	if err := config.Output.ValidateAndReset(); err != nil {
		return err
	}
	if config.Handle == nil {
		config.Handle = &Handle{
			Transform: []*Field{},
			FilterRow: []*Field{},
			FilterCol: []*Field{},
		}
	}
	if err := config.Handle.ValidateAndReset(); err != nil {
		return err
	}
	return nil
}

func (conf *Conf) ValidateAndReset() error {
	if conf.Batch == nil {
		*conf.Batch = defaultBatch
		log.Printf("Reset config [conf.batch] value to %d", conf.Batch)
		return nil
	}
	//if conf.Concurrency == nil {
	//	*conf.Concurrency = defaultConcurrency
	//	log.Printf("Reset config [conf.concurrency] value to %d", conf.Concurrency)
	//	return nil
	//}
	if _, ok := cast.ToIntE(conf.Batch); ok != nil {
		return fmt.Errorf("unable to parse [conf.batch] value of [%d]", conf.Batch)
	}
	//if _, ok := cast.ToIntE(conf.Concurrency); ok != nil {
	//	return fmt.Errorf("unable to parse [conf.concurrency] value of [%d]", conf.Concurrency)
	//} else {
	//	*conf.Concurrency = *conf.Concurrency - 10
	//}
	return nil
}

func (input *Input) ValidateAndReset() error {
	if input.FilePath == nil {
		return fmt.Errorf("no find [input.filePath] config")
	}
	if input.Delimiter == nil {
		*input.Delimiter = defaultDelimiter
		log.Printf("Reset config [input.delimiter] value to %s", *input.Delimiter)
	}
	if input.IsHeader == nil {
		*input.IsHeader = defaultIsHead
		log.Printf("Reset config [input.isHeader] value to %t", *input.IsHeader)
	}
	if _, ok := cast.ToBoolE(input.IsHeader); ok != nil {
		return fmt.Errorf("unable to parse [conf.batch] value of [%t]", *input.IsHeader)
	}
	return nil
}

func (output *Output) ValidateAndReset() error {
	if output.FilePath == nil {
		return fmt.Errorf("no find [output.filePath] config")
	}
	if output.FailPath == nil {
		return fmt.Errorf("no find [output.failPath] config")
	}
	if output.Delimiter == nil {
		*output.Delimiter = defaultDelimiter
		log.Printf("Reset config [output.delimiter] value to %s", *output.Delimiter)
	}
	return nil
}

func DetermineCol(col string, colName string) error {
	cv := strings.Split(col, "c")
	if len(cv) != 2 && cv[0] != "" {
		return fmt.Errorf("unable to parse [%s] value of [%s]", colName, col)
	}
	if _, ok := cast.ToIntE(cv[1]); ok != nil {
		return fmt.Errorf("unable to parse [%s] value of [%s]", colName, col)
	}
	return nil
}

func DetermineTimeParam(col *string, method string) error {
	v, ok := defaultTimeMap[*col]
	if !ok {
		return fmt.Errorf("unable to parse time pattern [%s] of [%s]", *col, method)
	} else {
		*col = v
	}
	return nil
}

func (handle *Handle) ValidateAndReset() error {
	if handle.Transform != nil && len(handle.Transform) != 0 {
		for _, field := range handle.Transform {
			if field.Col == nil || field.Method == nil {
				return fmt.Errorf("unable to parse [field.col,field.method] value of [%s,%s]", *field.Col, *field.Method)
			}
			if err := DetermineCol(*field.Col, "filed.col"); err != nil {
				return err
			}
			if field.Mode != nil {
				if _, ok := cast.ToIntE(field.Mode); ok != nil {
					*field.Mode = 0
					log.Printf("Reset config [handle.transform.field.Mode] value to %d in %s", 0, *field.Method)
				}
				if *field.Mode != 1 && *field.Mode != 0 {
					*field.Mode = 0
					log.Printf("Reset config [handle.transform.field.Mode] value to %d in %s", 0, *field.Method)
				}
			}
			if *field.Method == ToUpper || *field.Method == ToLower || *field.Method == TrimSpace {
				continue
			} else if *field.Method == JoinLeft || *field.Method == JoinRight {
				if field.Params == nil || len(field.Params) != 1 {
					return fmt.Errorf("inconsistent [field.params] count of [%s]", *field.Method)
				}
				continue
			} else if *field.Method == Replace {
				if field.Params == nil || len(field.Params) != 2 {
					return fmt.Errorf("inconsistent [field.params] count of [%s]", *field.Method)
				}
				continue
			} else if *field.Method == UnixToTime || *field.Method == TimeToUnix || *field.Method == TimeToUnixMilli {
				if field.Params == nil || len(field.Params) != 1 {
					return fmt.Errorf("inconsistent [field.params] count of [%s]", *field.Method)
				}
				if err := DetermineTimeParam(field.Params[0].Param, *field.Method); err != nil {
					return err
				}
				continue
			} else if *field.Method == TimeToTime {
				if field.Params == nil || len(field.Params) != 2 {
					return fmt.Errorf("inconsistent [field.params] count of [%s]", *field.Method)
				}
				if err := DetermineTimeParam(field.Params[0].Param, *field.Method); err != nil {
					return err
				}
				if err := DetermineTimeParam(field.Params[1].Param, *field.Method); err != nil {
					return err
				}
				continue
			} else {
				return fmt.Errorf("unable to parse [field.method] value of [%s]", *field.Method)
			}
		}
		return nil
	}
	if handle.FilterRow != nil && len(handle.FilterRow) != 0 {
		for _, field := range handle.FilterRow {
			if field.Col == nil || field.Method == nil || field.Value == nil {
				return fmt.Errorf("unable to parse [field.col,field.method,field.value] value of [%s, %s, %s]", *field.Col, *field.Method, *field.Value)
			}
			if *field.Method != GT && *field.Method != GTE && *field.Method != LT && *field.Method != LTE && *field.Method != EQ && *field.Method != NEQ {
				return fmt.Errorf("unable to parse [field.method] value of [%s]", *field.Method)
			}
			if err := DetermineCol(*field.Col, "filed.col"); err != nil {
				return err
			}
		}
	}
	if handle.FilterCol != nil && len(handle.FilterCol) != 0 {
		for _, field := range handle.FilterCol {
			if field.CutCol != nil {
				if err := DetermineCol(*field.CutCol, "filed.cutCol"); err != nil {
					return err
				}
			}
			if field.AddCol != nil {
				av := strings.Split(*field.AddCol, ":")
				if len(av) != 1 && len(av) != 2 {
					return fmt.Errorf("unable to parse [field.addCol] value of [%s]", *field.AddCol)
				}
				if len(av) == 1 && av[0] != RandomValue {
					return fmt.Errorf("unable to parse [field.addCol] value of [%s]", *field.AddCol)
				}
				if len(av) == 2 && (av[0] != FixedValue || av[0] != CurrentTime) {
					return fmt.Errorf("unable to parse [field.addCol] value of [%s]", *field.AddCol)
				}
			}
		}
	}
	return nil
}

func InitRead(conf TConfig) error {
	log.Printf("Init read.")
	inputPath := *conf.Input.FilePath
	_, err := os.Stat(inputPath)
	if os.IsNotExist(err) {
		return err
	}
	return nil
}

func InitWrite(conf TConfig) error {
	log.Printf("Init write.")

	outputPath := *conf.Output.FilePath
	_, err := os.Stat(outputPath)
	if err == nil {
		os.Remove(outputPath)
	}
	if err := os.MkdirAll(path.Dir(outputPath), 0775); err != nil && !os.IsExist(err) {
		return err
	}
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	outputFile.Close()

	failPath := *conf.Output.FailPath
	_, err1 := os.Stat(failPath)
	if err1 == nil {
		os.Remove(failPath)
	}
	if err := os.MkdirAll(path.Dir(failPath), 0775); err != nil && !os.IsExist(err) {
		return err
	}
	failFile, err := os.Create(failPath)
	if err != nil {
		return err
	}
	failFile.Close()

	return nil
}

func InitContext(conf *TConfig) (ContextMgr, error) {
	mgr := ContextMgr{
		TConfig:    conf,
		Statistics: &Statistics{},
	}
	mgr.Statistics.FinishLineNum = 0
	mgr.Statistics.FilterLineNum = 0
	mgr.Statistics.ErrorLineNum = 0
	//定义读和写执行状态channel，用于读和写任务通信
	mgr.RwNoticeCh = make(chan int)
	//定义读和写执行状态channel，用于响应读和写任务通信
	mgr.RwActNoticeCh = make(chan int)
	//定义读和写执行状态channel，用于传输关闭信号
	mgr.RwDoneCh = make(chan int)
	//定义统计channel，用于输出统计信息
	mgr.StateCh = make(chan int)

	//if *conf.Conf.Concurrency > 0 {
	//	for i := 0; i < *conf.Conf.Concurrency; i++ {
	//		mgr.ConcurrencyChs = append(mgr.ConcurrencyChs, make(chan []string, *conf.Conf.Batch*10))
	//	}
	//}

	rFields := conf.Handle.FilterRow
	rhs := HandleSchema{}
	rhs.Type = filterRow
	for _, rField := range rFields {
		fun, err := FuncParse(filterRow, *rField)
		if err != nil {
			return mgr, err
		}
		rhs.Function = append(rhs.Function, *fun)
	}
	mgr.HandleSchema = append(mgr.HandleSchema, rhs)

	tFields := conf.Handle.Transform
	ths := HandleSchema{}
	ths.Type = transform
	for _, tField := range tFields {
		fun, err := FuncParse(transform, *tField)
		if err != nil {
			return mgr, err
		}
		ths.Function = append(ths.Function, *fun)
	}
	mgr.HandleSchema = append(mgr.HandleSchema, ths)

	cFields := conf.Handle.FilterCol
	chs := HandleSchema{}
	chs.Type = filterCol
	for _, cField := range cFields {
		fun, err := FuncParse(filterCol, *cField)
		if err != nil {
			return mgr, err
		}
		chs.Function = append(chs.Function, *fun)
	}
	mgr.HandleSchema = append(mgr.HandleSchema, chs)

	return mgr, nil
}

func FuncParse(fType string, field Field) (*Function, error) {
	switch fType {
	case transform:
		function, err := TransformParse(field)
		return function, err
	case filterRow:
		function, err := FilterRowParse(field)
		return function, err
	case filterCol:
		function, err := FilterColParse(field)
		return function, err
	default:
		return nil, errors.New("unknown handle type")
	}
	return nil, nil
}

func TransformParse(field Field) (*Function, error) {
	if *field.Method == ToUpper || *field.Method == ToLower || *field.Method == TrimSpace { //无参
		function := &Function{}
		function.FuncName = *field.Method
		if field.Mode == nil || *field.Mode != 1 {
			function.FuncMode = 0
		} else {
			function.FuncMode = *field.Mode
		}
		colNum := strings.Replace(*field.Col, "c", "", -1)
		cv, err := cast.ToIntE(colNum)
		if err != nil {
			return nil, err
		}
		function.FuncColumn = cv
		return function, nil
	} else if *field.Method == JoinLeft || *field.Method == JoinRight || *field.Method == UnixToTime || *field.Method == TimeToUnix || *field.Method == TimeToUnixMilli { //1个参数
		function := &Function{}
		function.FuncName = *field.Method
		if field.Mode == nil || *field.Mode != 1 {
			function.FuncMode = 0
		} else {
			function.FuncMode = *field.Mode
		}
		colNum := strings.Replace(*field.Col, "c", "", -1)
		cv, err := cast.ToIntE(colNum)
		if err != nil {
			return nil, err
		}
		function.FuncColumn = cv
		pms := make([]string, 1)
		pms[0] = *field.Params[0].Param
		function.FuncParams = pms
		return function, nil
	} else if *field.Method == Replace || *field.Method == TimeToTime { //2个参数
		function := &Function{}
		function.FuncName = *field.Method
		if field.Mode == nil || *field.Mode != 1 {
			function.FuncMode = 0
		} else {
			function.FuncMode = *field.Mode
		}
		colNum := strings.Replace(*field.Col, "c", "", -1)
		cv, err := cast.ToIntE(colNum)
		if err != nil {
			return nil, err
		}
		function.FuncColumn = cv
		pms := make([]string, 2)
		pms[0] = *field.Params[0].Param
		pms[1] = *field.Params[1].Param
		function.FuncParams = pms
		return function, nil
	} else {
		return nil, errors.New("unknown transform function")
	}
}

func FilterRowParse(field Field) (*Function, error) {
	colNum := strings.Replace(*field.Col, "c", "", -1)
	cv, err := cast.ToIntE(colNum)
	if err != nil {
		return nil, err
	}
	function := &Function{}
	function.FuncColumn = cv
	if *field.Method == EQ || *field.Method == NEQ {
		function.FuncName = *field.Method
		pms := make([]string, 1)
		pms[0] = *field.Value
		function.FuncParams = pms
	} else if *field.Method == GT || *field.Method == GTE || *field.Method == LT || *field.Method == LTE {
		function.FuncName = *field.Method
		pattern := "^[0-9]+$"
		_, err := regexp.MatchString(pattern, *field.Value)
		if err != nil {
			return nil, fmt.Errorf("unknown filterRow.field.value of %s", *field.Method)
		}
		pms := make([]string, 1)
		pms[0] = *field.Value
		function.FuncParams = pms
	} else {
		return nil, errors.New("unknown filterRow function")
	}
	return function, nil
}

func FilterColParse(field Field) (*Function, error) {
	function := &Function{}
	if field.AddCol != nil {
		vs := strings.Split(*field.AddCol, ":")
		if vs[0] == RandomValue {
			function.FuncName = RandomValue
			function.FuncMode = 1
		} else if vs[0] == CurrentTime {
			function.FuncName = CurrentTime
			function.FuncMode = 1
		} else if vs[0] == FixedValue {
			function.FuncName = FixedValue
			function.FuncMode = 1
			pms := make([]string, 1)
			pms[0] = vs[1]
			function.FuncParams = pms
		}
		return function, nil
	}
	if field.CutCol != nil {
		colNum := strings.Replace(*field.CutCol, "c", "", -1)
		cv, err := cast.ToIntE(colNum)
		if err != nil {
			return nil, err
		}
		function.FuncName = CUT
		function.FuncColumn = cv
		return function, nil
	}
	return nil, errors.New("filterCol param exception")
}

func ReadFile(contextMgr ContextMgr, dataCh chan string, errDataCh chan string) error {
	log.Printf("Goroutine Reading file.")
	defer wg.Done()
	defer close(contextMgr.RwNoticeCh)
	defer close(errDataCh)
	defer close(dataCh)
	//defer func() {
	//	for _, handleCh := range contextMgr.ConcurrencyChs {
	//		close(handleCh)
	//	}
	//}()
	filePath := *contextMgr.TConfig.Input.FilePath
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	defer file.Close()
	if err != nil {
		return err
	}
	reader := csv.NewReader(bufio.NewReader(file))
	de := []rune(*contextMgr.TConfig.Input.Delimiter)
	reader.Comma = de[0]
	readLine := 0
	noticeBatch := *contextMgr.TConfig.Conf.Batch * 1000
	wg.Add(1)
	go func() {
		log.Printf("Goroutine monitor stage write single.")
		defer func() {
			wg.Done()
			defer close(contextMgr.StateCh)
		}()
		sendTag := 0
		for {
			_, ok := <-contextMgr.RwActNoticeCh
			if !ok {
				break
			}
			sendTag++
			//因正常写和错误写都会发响应通道，故当两者都收到时，发送打印指令给打印通道
			if sendTag%2 == 0 {
				contextMgr.StateCh <- 0
			}
		}
	}()

	//for i, handleCh := range contextMgr.ConcurrencyChs {
	//	wg.Add(1)
	//	log.Printf("Goroutine handle concurrency-%d.", i)
	//	go ConHandle(contextMgr, dataCh, errDataCh, handleCh)
	//}

	for {
		line, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		readLine++
		if readLine%noticeBatch == 0 {
			contextMgr.RwNoticeCh <- 0
			contextMgr.Statistics.StageFilterTime = time.Now().Unix()
		}

		if readLine == 1 && *contextMgr.TConfig.Input.IsHeader {
			dataCh <- strings.Join(line, *contextMgr.TConfig.Output.Delimiter)
			continue
		}

		//handleCh := contextMgr.ConcurrencyChs[readLine%len(contextMgr.ConcurrencyChs)]
		//handleCh <- line

		handleLine, err := handle(contextMgr, line)
		if err != nil {
			errDataCh <- handleLine
		} else {
			if handleLine != "" {
				dataCh <- handleLine
			} else {
				contextMgr.Statistics.FilterLineNum++
			}
		}
	}
	return nil
}

func ConHandle(contextMgr ContextMgr, dataCh chan string, errDataCh chan string, handleCh chan []string) {
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			data, ok := <-handleCh
			if !ok {
				contextMgr.RwDoneCh <- 0
				break
			}
			handleLine, err := handle(contextMgr, data)
			if err != nil {
				errDataCh <- handleLine
			} else {
				if handleLine != "" {
					dataCh <- handleLine
				} else {
					contextMgr.Statistics.FilterLineNum++
				}
			}
		}
	}()
}

func handle(contextMgr ContextMgr, line []string) (lineStr string, err error) {
	var addLine []string
	//备份原始数据，复制一份原始数据
	bakLine := make([]string, len(line))
	copy(bakLine[:], line[:])
	for _, schema := range contextMgr.HandleSchema {
		switch schema.Type {
		case transform:
			for _, function := range schema.Function {
				if (function.FuncColumn + 1) > len(bakLine) {
					return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("index out of range")
				}
				var upValue string
				switch function.FuncName {
				case ToUpper:
					upValue = strings.ToUpper(bakLine[function.FuncColumn])
				case ToLower:
					upValue = strings.ToLower(bakLine[function.FuncColumn])
				case TrimSpace:
					upValue = strings.TrimSpace(bakLine[function.FuncColumn])
				case Replace:
					upValue = strings.Replace(bakLine[function.FuncColumn], function.FuncParams[0], function.FuncParams[1], -1)
				case JoinLeft:
					upValue = function.FuncParams[0] + bakLine[function.FuncColumn]
				case JoinRight:
					upValue = bakLine[function.FuncColumn] + function.FuncParams[0]
				case TimeToUnix:
					t, err := time.Parse(function.FuncParams[0], bakLine[function.FuncColumn])
					if err != nil {
						return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("time parse exception")
					} else {
						upValue = cast.ToString(t.Unix())
					}
				case TimeToUnixMilli:
					t, err := time.Parse(function.FuncParams[0], bakLine[function.FuncColumn])
					if err != nil {
						return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("time parse exception")
					} else {
						upValue = cast.ToString(t.UnixNano() / 1e6)
					}
				case UnixToTime:
					by := []byte(bakLine[function.FuncColumn])
					if len(by) > 10 {
						by = by[0:10]
					}
					t, err := cast.ToInt64E(string(by))
					if err != nil {
						return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("time parse exception")
					} else {
						upValue = cast.ToTime(t).Format(function.FuncParams[0])
					}
				case TimeToTime:
					t, err := time.Parse(function.FuncParams[0], bakLine[function.FuncColumn])
					if err != nil {
						return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("time parse exception")
					} else {
						upValue = time.Unix(t.Unix(), 0).Format(function.FuncParams[1])
					}
				}
				if function.FuncMode == 1 {
					addLine = append(addLine, upValue)
				} else {
					line[function.FuncColumn] = upValue
				}
			}
		case filterRow:
			for _, function := range schema.Function {
				if (function.FuncColumn + 1) > len(bakLine) {
					return strings.Join(bakLine, *contextMgr.TConfig.Input.Delimiter), errors.New("index out of range")
				}
				srcValue := bakLine[function.FuncColumn]
				dstValue := function.FuncParams[0]
				switch function.FuncName {
				case EQ:
					if srcValue != dstValue {
						return "", nil
					}
				case NEQ:
					if srcValue == dstValue {
						return "", nil
					}
				case GT:
					if srcValue <= dstValue {
						return "", nil
					}
				case GTE:
					if srcValue < dstValue {
						return "", nil
					}
				case LT:
					if srcValue >= dstValue {
						return "", nil
					}
				case LTE:
					if srcValue > dstValue {
						return "", nil
					}
				}
			}
		case filterCol:
			var cutLines []string
			for _, function := range schema.Function {
				switch function.FuncName {
				case CUT:
					if (function.FuncColumn + 1) > len(line) {
						return "", errors.New("index out of range")
					} else {
						cutLines = append(cutLines, bakLine[function.FuncColumn])
					}
				case FixedValue:
					addLine = append(addLine, function.FuncParams[0])
				case RandomValue:
					addLine = append(addLine, cast.ToString(rand.Int63n(time.Now().Unix())))
				case CurrentTime:
					addLine = append(addLine, cast.ToString(time.Now().Unix()))
				}
			}
			if len(cutLines) != 0 {
				line = cutLines
			}
		}
	}
	line = append(line, addLine...)
	outDelimiter := *contextMgr.TConfig.Output.Delimiter
	lineStr = strings.Join(line, outDelimiter)

	return lineStr, err
}

func WriteFile(contextMgr ContextMgr, dataCh chan string) error {
	log.Printf("Goroutine writing file.")
	defer wg.Done()
	filePath := *contextMgr.TConfig.Output.FilePath
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		return err
	}
	dataNums := writeOpt(file, contextMgr, dataCh, CommonType)
	contextMgr.Statistics.FinishLineNum = dataNums
	return nil
}

func WriteErrDataFile(contextMgr ContextMgr, errDataCh chan string) error {
	log.Printf("Goroutine writing err file.")
	defer wg.Done()
	filePath := *contextMgr.TConfig.Output.FailPath
	file, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		return err
	}
	errDataNums := writeOpt(file, contextMgr, errDataCh, ErrorType)
	contextMgr.Statistics.ErrorLineNum = errDataNums
	return nil
}

func writeOpt(file io.Writer, contextMgr ContextMgr, dataCh chan string, wType string) int {
	write := bufio.NewWriter(file)
	batch := *contextMgr.TConfig.Conf.Batch
	lineNum := 0
	wg.Add(1)
	go func() {
		log.Printf("Goroutine monitor stage read single of %s.", wType)
		defer func() {
			wg.Done()
		}()
		for {
			//接收通道
			_, ok := <-contextMgr.RwNoticeCh
			if !ok {
				if wType == CommonType {
					contextMgr.RwDoneCh <- 1
				} else if wType == ErrorType {
					contextMgr.RwDoneCh <- 2
				}
				break
			}
			if wType == CommonType {
				contextMgr.Statistics.StageFinishTime = time.Now().Unix()
				contextMgr.Statistics.FinishLineNum = lineNum
			} else if wType == ErrorType {
				contextMgr.Statistics.StageErrorTime = time.Now().Unix()
				contextMgr.Statistics.ErrorLineNum = lineNum
			}
			//响应通道
			contextMgr.RwActNoticeCh <- 0
		}
	}()
	for {
		data, ok := <-dataCh
		if !ok {
			break
		}
		lineNum++
		write.WriteString(data)
		write.WriteString("\r\n")
		if lineNum%batch == 0 {
			write.Flush()
		}
	}
	write.Flush()
	return lineNum
}

func PrintStatistics(contextMgr ContextMgr) {
	log.Printf("Goroutine print statistics.")
	defer wg.Done()
	for {
		_, ok := <-contextMgr.StateCh
		if !ok {
			break
		}
		finishedNum := contextMgr.Statistics.FinishLineNum
		filterNum := contextMgr.Statistics.FilterLineNum
		errorNum := contextMgr.Statistics.ErrorLineNum
		stageTotal := finishedNum + filterNum + errorNum
		stageCost := time.Now().Unix() - contextMgr.Statistics.StartReadTime

		log.Printf("Stage: Finished(%d), Filter(%d), Failed(%d), Rows AVG(%.2f/s).",
			finishedNum, filterNum, errorNum, float64(stageTotal)/cast.ToFloat64(stageCost))
	}
}

func CloseRwChannel(contextMgr ContextMgr) {
	log.Printf("Goroutine close rwChannel.")
	go func() {
		defer wg.Done()
		defer close(contextMgr.RwDoneCh)
		m := make(map[int]struct{})
		for i := range contextMgr.RwDoneCh {
			m[i] = struct{}{}
			if len(m) == 2 {
				close(contextMgr.RwActNoticeCh)
				break
			}
		}
	}()
}
