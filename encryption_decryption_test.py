from Crypto.Cipher import AES
import base64

secret_key = '1234589648789658'
cipher = AES.new(secret_key, AES.MODE_ECB)


