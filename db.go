package db

import (
	"context"

	"github.com/allanwei/gcwebapis-config"

	//Add mssql driver
	"database/sql"
	//Add mssql driver
	_ "github.com/denisenkom/go-mssqldb"
	//Add mssql driver
	//_ "github.com/jinzhu/gorm/dialects/mssql"
)

//DBcon ... Project DB type
type DBcon struct {
	DB    *sql.DB
	Ctx   context.Context
	Qstmt *sql.Stmt
}

//ExecuteResult ...
type ExecuteResult struct {
	LastInsertId int64 `json:"last_insert_id,omitempty"`
	RowsAffected int64 `json:"rows_affected,omitempty"`
}

//ExecuteQuery ...
func (c *DBcon) ExecuteQuery(sqlquery string) (*ExecuteResult, error) {
	err := c.DB.PingContext(c.Ctx)
	if err != nil {
		return nil, err
	}
	c.Qstmt, err = c.DB.PrepareContext(c.Ctx, sqlquery)
	if err != nil {
		return nil, err
	}
	defer c.Qstmt.Close()
	r, err := c.Qstmt.ExecContext(c.Ctx)
	if err != nil {
		return nil, err
	}
	iid, err := r.LastInsertId()
	if err != nil {
		iid = 0
	}
	rc, err := r.RowsAffected()
	if err != nil {
		rc = 0
	}
	return &ExecuteResult{LastInsertId: iid, RowsAffected: rc}, nil
}

//GetResultRow ...
func (c *DBcon) GetResultRow(sqlquery string) (*sql.Row, error) {
	//defer c.DB.Close()

	err := c.DB.PingContext(c.Ctx)
	if err != nil {
		return nil, err
	}
	c.Qstmt, err = c.DB.PrepareContext(c.Ctx, sqlquery)
	if err != nil {
		return nil, err
	}

	defer c.Qstmt.Close()
	row := c.Qstmt.QueryRowContext(c.Ctx)
	return row, nil

}

//GetResultRows ...
func (c *DBcon) GetResultRows(sqlquery string) (*sql.Rows, error) {
	//defer c.DB.Close()

	err := c.DB.PingContext(c.Ctx)
	if err != nil {
		return nil, err
	}
	c.Qstmt, err = c.DB.PrepareContext(c.Ctx, sqlquery)
	if err != nil {
		return nil, err
	}

	defer c.Qstmt.Close()
	rows, err := c.Qstmt.QueryContext(c.Ctx)
	if err != nil {
		return nil, err
	}
	return rows, nil

}
//Close ... Close dbConntions
func (c *DBcon) Close(){
	c.DB.Close()
	c.Ctx.Done()
	c.Qstmt.Close()
}
//CreateDBCon ... Create Project DB Connection
func CreateDBCon(dbconnectionstring *string) (*DBcon, error) {
	var cs string
	var err error
	if dbconnectionstring != nil {
		cs = *dbconnectionstring
	} else {
		cs, err = config.GetDatabaseConf()
		if err != nil {
			return nil, err
		}
	}

	db, err := sql.Open("mssql", cs)
	if err != nil {
		return nil, err
	}
	//Set db MaxIdleConns
	//db.SetMaxIdleConns(10)
	//Set db MaxOpenConns
	//db.SetMaxOpenConns(100)
	//defer db.Close()
	// make sure connection is available
	//ctx := context.Background()
	//err = db.Ping(Context(ctx))
	err = db.Ping()

	if err != nil {
		return nil, err

	}
	c := DBcon{DB: db, Ctx: context.Background()}
	return &c, nil
}
