package sqlmapper

/*
    该文件封装了下sql_mapper，只需要初始化一个MysqlBase实例，即可调用增删改查等接口
    无需编写sql语句，删改查都提供了指定字段，方便使用。
    todo 批量添加、修改、删除、查询
 */

import (
	"database/sql"
	"fmt"
)

type MysqlBase struct {
	TableName string
	Db        *sql.DB
}

type TestStruct struct {
	Name string `sql:"name"`
	Age  int64  `sql:"age"`
	Sex  string `sql:"sex"`
}

/*
    根据结构体添加数据到数据库
  [in] info: 需要添加的结构体，应该是个指针，否则会报错。
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) AddInfo(info interface{}) (rowAffected int64, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowAffected, err = fm.SQLInsert(self.Db)
	if err != nil {
		fmt.Printf("sql insert failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体首个字段修改结构体数据到数据库(PrimaryKey指首个字段)
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) PutInfoByPrimaryKey(info interface{}) (rowAffected int64, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowAffected, err = fm.SQLUpdateByPriKey(self.Db)
	if err != nil {
		fmt.Printf("sql update failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体某个字段修改结构体数据到数据库
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [in] nameInDb: 结构体某个字段名
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) PutInfoByFieldNameInDB(info interface{}, nameInDb string) (rowAffected int64, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowAffected, err = fm.SQLUpdateByFieldNameInDB(self.Db, nameInDb)
	if err != nil {
		fmt.Printf("sql update failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体首个字段从数据库中删除
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) DeleteInfoByPrimaryKey(info interface{}) (rowAffected int64, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowAffected, err = fm.SQLDeleteByPriKey(self.Db)
	if err != nil {
		fmt.Printf("sql delete failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体某个字段从数据库中删除
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [in] nameInDb: 结构体某个字段名
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) DeleteInfoByFieldNameInDB(info interface{}, nameInDb string) (rowAffected int64, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowAffected, err = fm.SQLDeleteByFieldNameInDB(self.Db, nameInDb)
	if err != nil {
		fmt.Printf("sql delete failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体首个字段查询数据库
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) GetInfoByPrimaryKey(info interface{}) (rowObj interface{}, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowObj, err = fm.SQLSelectByPriKey(self.Db)
	if err != nil {
		fmt.Printf("sql get failed. err:%s\n", err.Error())
	}
	return
}

/*
    通过结构体某个字段查询数据库
  [in] info: 需要修改的结构体，应该是个指针，否则会报错。
  [in] nameInDb: 结构体某个字段名
  [out] rowaffected: 受影响的行数
  [out] error 错误信息
 */
func (self *MysqlBase) GetInfosByFieldNameInDB(info interface{}, nameInDb string) (rowObjs []interface{}, err error) {
	fm, err := NewFieldsMap(self.TableName, info)
	if err != nil {
		fmt.Printf("create new fields map failed.err:%s\n", err.Error())
		return
	}
	rowObjs, err = fm.SQLSelectRowsByFieldNameInDB(self.Db, nameInDb)
	if err != nil {
		fmt.Printf("sql get failed. err:%s\n", err.Error())
	}
	return
}

// 简单使用
func main() {
	db, err := sql.Open("mysql", "lgh:linguohao@(192.168.50.129:3306)/test")
	if err != nil {
		fmt.Println("open mysql failed.err", err.Error())
		return
	}
	mysqlBase := MysqlBase{
		"test",
		db,
	}
	test := &TestStruct{
		Name: "link",
		Age: 30,
		Sex: "male",
	}
	mysqlBase.AddInfo(test)
}