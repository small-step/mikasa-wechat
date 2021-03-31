### C++ DLL注入器

因为hook的dll是32位的，所以只能调用32位的exe或者dll去注入，因为python调用dll也要匹配版本，所以还是用vs生成x86的exe应用程序来注入。

### 源码

来自项目 [cixingguangming55555/wechat-bot](https://github.com/cixingguangming55555/wechat-bot)