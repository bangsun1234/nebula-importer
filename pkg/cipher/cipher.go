package cipher

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

func Md5Encode(data string) string {
	h := md5.New()
	h.Write([]byte(data))
	return hex.EncodeToString(h.Sum(nil))
}

func HmacSha256(message string, secret string) string {
	key := []byte(secret)
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func Aes256Encrypt(orig string, key string) string {
	//转成字节数组
	origData := []byte(orig)
	//秘钥补全
	k := SecretKeyPadding([]byte(key))
	//分组秘钥
	block, _ := aes.NewCipher(k)
	//获取秘钥块的长度
	blocksize := block.BlockSize()
	//补全码
	origData = PKCS7Padding(origData, blocksize)
	//加密模式
	blockMode := cipher.NewCBCEncrypter(block, k[:blocksize])
	//创建数组
	cryted := make([]byte, len(origData))
	//加密
	blockMode.CryptBlocks(cryted, origData)
	return base64.StdEncoding.EncodeToString(cryted)
}

func Aes256Decrypt(cryted string, key string) string {
	//转成字节数组
	crytedByte, _ := base64.StdEncoding.DecodeString(cryted)
	//秘钥补全
	k := SecretKeyPadding([]byte(key))
	//分组秘钥
	block, _ := aes.NewCipher(k)
	//获取秒钥块的长度
	blocksize := block.BlockSize()
	//加密模式
	blockMode := cipher.NewCBCDecrypter(block, k[:blocksize])
	//创建数组
	orig := make([]byte, len(crytedByte))
	//解密
	blockMode.CryptBlocks(orig, crytedByte)
	//去补全码
	orig = PKCS7UnPadding(orig)
	return string(orig)
}

func PKCS7Padding(ciphertext []byte, blocksize int) []byte {
	padding := blocksize - len(ciphertext)%blocksize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func SecretKeyPadding(secretKey []byte) []byte {
	secretLen := len(secretKey)
	if secretLen > 32 {
		secretKey = secretKey[0:32]
	} else {
		size := 32 / secretLen
		secretKey = bytes.Repeat(secretKey, size+1)[0:32]
		//secretKey = PKCS7Padding(secretKey, 32)
	}
	return secretKey
}
