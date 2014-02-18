;; function without arguments
(auth.logOut)
;;user define type with named args + int parsing+serialization test
(messages.getDialogs ("offset" -213932) ("max_id" 6) ("limit" 10))
;;it is possible to change order of named arguments
(messages.getDialogs limit:10 offset:-213932 max_id:6)
;; string test
(auth.checkPhone ("phone_number" "123456789"))
;; Vector<int> test
(messages.getMessages ("id" [1 2 3 10]))
;; another colon separated syntax
;; SCHEME lexer will automatically change it to the previous form
(messages.getMessages id:[1 2 3 10])

;; user defined type
(users.getUsers ("id" [(inputUserEmpty)]))
(users.getUsers ("id" [(inputUserEmpty) (inputUserSelf) (inputUserContact ("user_id" 6))]))

;; string test (escape sequences)
(auth.checkPhone ("phone_number" "\n\r\t\"\\\x01\x7f"))

;; long test1
(photos.updateProfilePhoto id:1234567890 (inputPhotoCrop crop_left:1 crop_top:1 crop_width:10))

;;anonymous RPC-function argument
(messages.getMessages [1 2 3 10])

(help.getScheme 0xabacaba)
