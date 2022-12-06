package cipher

import (
	"fmt"
	"testing"
)

func TestMd5Encode(t *testing.T) {
	msg := "This is password!"
	fmt.Println(Md5Encode(msg))
}

func TestHmacSha256(t *testing.T) {
	msg := "8ang$un"
	secret := "YmFuZ3N1bg=="
	enc := HmacSha256(msg, secret)
	fmt.Println(enc)
	fmt.Println(len(enc))
}

func TestAes256(t *testing.T) {
	orig := "nebula"
	key := "YmFuZ3N1bg=="
	fmt.Println("原文：", orig)

	encryptCode := Aes256Encrypt(orig, key)
	fmt.Println("密文：", encryptCode)

	decryptCode := Aes256Decrypt(encryptCode, key)
	fmt.Println("解密结果：", decryptCode)
}
