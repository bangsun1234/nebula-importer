//Transform操作
Transform操作主要是用于对数据进行转换
ToUpper
功能描述：字段大写
参数描述：无参
示例：App->APP

ToLower
功能描述：字段小写
参数描述：无参
示例：App->app

Replace
功能描述：字段的str1替换成str2
参数描述：参数1<需要被替换的str1>,参数2<替换成的str2>
示例：App，参数1：p,参数2：m ->Amm

TrimSpace
功能描述：字段去除空格
参数描述：无参
示例： App ->App

JoinLeft
功能描述：字段左拼接str1
参数描述：参数1<拼接字符串str1>
示例：App，参数1：Start  ->StartApp

JoinRight
功能描述：字段右拼接str1
参数描述：参数1<拼接字符串str1>
示例：App，参数1：End  ->AppEnd

TimeToUnix
功能描述：字段格式时间转成时间戳
参数描述：参数1<时间格式yyyy-mm-dd hh:mm:ss,yyyy/mm/dd hh:mm:ss,yyyy-mm-dd,yyyy/mm/dd>
示例：2006-01-02 15:04:05，参数1：yyyy-mm-dd hh:mm:ss  ->1136185445

UnixToTime
功能描述：字段按str1格式转换
参数描述：参数1<时间格式yyyy-mm-dd hh:mm:ss,yyyy/mm/dd hh:mm:ss,yyyy-mm-dd,yyyy/mm/dd>
示例：1136185445，参数1：yyyy-mm-dd hh:mm:ss  ->2006-01-02 15:04:05

TimeToTime
功能描述：字段按str1格式转换成str2格式
参数描述：参数1<时间格式yyyy-mm-dd hh:mm:ss,yyyy/mm/dd hh:mm:ss,yyyy-mm-dd,yyyy/mm/dd>，参数2<时间格式yyyy-mm-dd hh:mm:ss,yyyy/mm/dd hh:mm:ss,yyyy-mm-dd,yyyy/mm/dd>
示例：2006-01-02 15:04:05，参数1：yyyy-mm-dd hh:mm:ss, 参数1：yyyy/mm/dd,  ->2006/01/02



//FilterRow操作
FilterRow操作主要是用于过滤数据
GT
功能描述：过滤大于阈值的记录（保留）
参数描述：参数1<阈值（数值型）>

GTE
功能描述：过滤大于等于阈值的记录（保留）
参数描述：参数1<阈值（数值型）>

LT
功能描述：过滤小于阈值的记录（保留）
参数描述：参数1<阈值（数值型）>

LTE
功能描述：过滤小于等于阈值的记录（保留）
参数描述：参数1<阈值（数值型）>

EQ
功能描述：过滤等于阈值的记录（保留）
参数描述：参数1<阈值（数值型，字符型，布尔型）>

NEQ
功能描述：过滤不等于阈值的记录（保留）
参数描述：参数1<阈值（数值型，字符型，布尔型）>

//FilterCol操作
FilterCol操作主要是用于裁剪数据列或增加数据列
配置项cutCol：需要保留的列号，未配置即保留全部列
配置项addCol：需要增加的数据列，提供三种模式<"FixedValue:$value":增加固定列,列值为value;"RandomValue":增加随机数列;"CurrentTime";增加当前时间戳列>
