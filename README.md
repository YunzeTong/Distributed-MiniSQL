# README

## 视频链接
由于视频过大不方便上传，这里提供永久阿里云盘链接
「视频.mov」https://www.aliyundrive.com/s/9MQ3n38UMJG 提取码: ub68
点击链接保存，或者复制本段内容，打开「阿里云盘」APP ，无需下载极速在线查看，视频原画倍速播放。
## 限制与假设

- master 不会挂
- 集群大小为 5，包括一个 master 服务器和 4 个 region 服务器（主服务器和从服务器各 2 个）。
- 集群中各个服务器的 IP 地址与监听端口不会发生变化。
- 容错容灾对象包括 region server crash、进程（特指分布式 MiniSQL 进程）crash、network partition。只要 master 所在的 partition（不管有没有发生 network partition）中服务器数量为 3 台及以上即可正常工作。

## 运维注意事项

### Setup

1. 在每个 region 上配置 FTP 服务器。

   安装 vsftpd。

   ```
   apt update
   apt install vsftpd
   ```

   创建用户，密码设为`tyz`。

   ```
   adduser tyz
   ```

   编辑`/etc/vsftpd.conf`，把下面这行的注释去掉。

   ```
   write_enable=YES
   ```

   留意`/etc/vsftpd.conf`中的`/var/run/vsftpd/empty`，创建以下文件夹。

   ```
   mkdir /var/run/vsftpd
   mkdir /var/run/vsftpd/empty
   ```

2. 安装 etcd。

   ```
   cd ~
   git clone -b v3.4.16 https://github.com/etcd-io/etcd.git
   cd etcd
   ./build
   export PATH="$PATH:`pwd`/bin"
   ```

3. 配置 git 避免 git 操作需要输入用户名和密码。

   ```
   git config --global credential.helper store
   ```

   随后进行任意 git 操作，输入用户名和密码，运行脚本时就不需要输了。

4. 在`/home/tyz`下把 repository pull 下来。

   ```
   cd /home/tyz
   git clone https://gitee.com/zhou-zheng-BevisChou/distributed-mini-sql.git
   ```

### 运行

1. 所有 region 上启动 FTP 服务器。

   ```
   vsftpd
   ```

2. master 和所有 region 上开启 etcd 进程

   ```
   # etcd (take region 1 for example)
   export THIS_NAME=host1
   export THIS_IP=172.18.0.3
   cd /home/tyz/distributed-mini-sql
   ./scripts/etcd.sh
   ```

3. 开启 master/region 进程。

   ```
   # master
   export THIS_NAME=host0
   export THIS_IP=172.18.0.2
   cd /home/tyz/distributed-mini-sql
   ./scripts/master.sh
   
   # region 
   export THIS_NAME=host1
   export THIS_IP=172.18.0.3
   cd /home/tyz/distributed-mini-sql
   ./scripts/region.sh
   ```

4. 开启 client 进程。

   ```
   # client
   cd /home/tyz/distributed-mini-sql
   ./scripts/client.sh
   ```

## Reference

- 关于集群大小

  参考了 etcd 的[官方建议](https://etcd.io/docs/v3.5/faq/)如下。

  > Theoretically, there is no hard limit. However, an etcd cluster probably should have no more than seven nodes. [Google Chubby lock service](http://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf), similar to etcd and widely deployed within Google for many years, suggests running five nodes. A 5-member etcd cluster can tolerate two member failures, which is enough in most cases.