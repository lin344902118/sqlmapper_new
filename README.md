# sqlmapper_new
this is a fork from sqlmapper
源地址：https://github.com/arthas29/sqlmapper
对于sqlmapper简单介绍
通过反射解析传入的结构体，然后根据结构体的字段构造sql语句。
适用于简单的增删改查等简单的sql操作。复杂操作请自己写sql语句。
由于源代码有些难用，做了一定修改。
增加部分：
增加getTagIndex，通过字段名称获取索引
增加SQLUpdateByFieldIndex，通过指定字段索引更新数据
增加SQLUpdateByFieldNameInDB，通过指定字段更新数据
增加SQLDeleteByFieldIndex，通过指定字段索引删除数据
增加SQLDeleteByFieldNameInDB，通过指定字段删除数据
删除部分：
取消所有context
修改部分：
修改insert，返回自增编号
修改SQLUpdateByPriKey，返回影响行数
修改SQLDeleteByPriKey，返回影响行数

如何使用见测试文件。
关于修改结构体部分字段而非全部字段的方法
1、自己写sql语句
2、重新建立结构体，只包含修改部分字段
3、先查询数据，修改需要修改的字段，然后更新
推荐使用2和3