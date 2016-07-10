# IMF
Quick setup — if you’ve done this kind of thing before or 

# HTTPS  SSH

  https://github.com/15751064254/IMF.git

  We recommend every repository include a README, LICENSE, and .gitignore.
  …or create a new repository on the command line

  echo "# IMF" >> README.md
  git init
  git add README.md
  git commit -m "first commit"
  git remote add origin https://github.com/15751064254/IMF.git
  git push -u origin master
  …or push an existing repository from the command line

  git remote add origin https://github.com/15751064254/IMF.git
  git push -u origin master
  …or import code from another repository
  You can initialize this repository with code from a Subversion, Mercurial, or TFS project.

  Import code
   ProTip! Use the URL for this page when adding GitHub as a remote.


#Git全局配置和单个仓库的用户名邮箱配置学习git的时候, 大家刚开始使用之前都配置了一个全局的用户名和邮箱
$ git config --global user.name "github's Name"
$ git config --global user.email "github@xx.com"
$ git config --list
 
#如果你公司的项目是放在自建的gitlab上面, 如果你不进行配置用户名和邮箱的话, 则会使用全局的, 这个时候是错误的, 正确的做法是针对公司的项目, 在项目根目录下进行单独配置
$ git config user.name "gitlab's Name"
$ git config user.email "gitlab@xx.com"
$ git config --list
$ git config --list查看当前配置, 在当前项目下面查看的配置是全局配置+当前项目的配置, 使用的时候会优先使用当前项目的配置
