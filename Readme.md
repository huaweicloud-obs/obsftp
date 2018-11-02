> # 简介
OBS FTP工具是一个特殊FTP server, 它接收普通FTP请求后，将对文件、文件夹的操作映射为对对象存储的业务操作，从而使得您可以基于FTP协议来管理存储在OBS上的文件。
***
> ## 说明
生产环境请使用obs sdk, ObsFTP工具主要面向个人用户使用。
***
> ### 主要特性
* 跨平台：Windows、Linux还是Mac， 32位还是64位操作系统，都可以运行,当前servr仅支持以命令行启动。
* 免安装： 解压后可直接运行。
* 免设置：无需设置即可运行。
* 透明化： FTP工具是python写的，您可以看到完整的源码，我们稍后也会开源到Github。

***

> ### 主要功能
*  支持文件和文件夹的上传、下载、删除等操作。
* 通过Multipart方式，分片上传大文件。
* 支持大部分FTP指令，可以满足日常FTP的使用需求。

>> ####     说明
***
>>>
1. 目前在1.0版本中，考虑到安装部署的简便，OSS FTP工具没有支持TLS加密。由于FTP协议是明文传输的，为了防止您的密码泄漏，建议将FTP server和client运行在同一台机器上，通过127.0.0.1:port的方式来访问。
2. 不支持rename和move操作。
3. 安装包解压后的路径不要含有中文。
4. FTP server支持的Python版本：Python2.6和Python2.7,需要提前在系统安装好，windows需要先下载安装。Linux当前都有自带的python可以直接使用。

> # 运行

* Windows: 打开 cmd 窗口，cd 进入 工具目录；运行python FTPServer.py .

``` cmd

Microsoft Windows [版本 6.1.7601]
版权所有 (c) 2009 Microsoft Corporation。保留所有权利。

C:\Users\****>cd D:\CloudStorage\StorageSolution\OBS_Demo\obsftp1.0.0-Windo
ws\OBSFTP

C:\Users\******>d:

D:\CloudStorage\StorageSolution\OBS_Demo\obsftp1.0.0-Windows\OBSFTP> python FTPS
erverStart.py
[I 18-09-20 22:37:10] >>> starting FTP server on 127.0.0.1:2048, pid=23380 <<<
[I 18-09-20 22:37:10] poller: <class 'pyftpdlib.ioloop.Select'>
[I 18-09-20 22:37:10] masquerade (NAT) address:
[I 18-09-20 22:37:10] passive ports: None

``` 



Linux： 打开终端，运行
``` shell
$ python FTPServerStart.py
``` 

> # 连接到FTP server
请使用FileZilla客户端去连接FTP server。下载安装后，按如下方式连接即可:

+ 主机: 127.0.0.1:2048 (默认)
- 登录类型： 正常
+ 用户：access_key_id/bucket_name
+ 密码：access_key_secret
