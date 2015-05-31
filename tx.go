package pg

import (
	"fmt"
	"github.com/golang/glog"
	"runtime"
)

// Not thread-safe.
type Tx struct {
	db  *DB
	_cn *conn

	err  error
	done bool
}

func (db *DB) Begin() (*Tx, error) {
	cn, err := db.conn()
	if err != nil {
		return nil, err
	}

	tx := &Tx{
		db:  db,
		_cn: cn,
	}
	if _, err := tx.Exec("BEGIN"); err != nil {
		tx.close()
		return nil, err
	}
	runtime.SetFinalizer(tx, txFinalizer)
	return tx, nil
}

// RunInTransaction runs a function in a transaction. If function
// returns an error transaction is rollbacked, otherwise transaction
// is committed.
func (db *DB) RunInTransaction(fn func(*Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (tx *Tx) conn() *conn {
	tx._cn.SetReadTimeout(tx.db.opt.ReadTimeout)
	tx._cn.SetWriteTimeout(tx.db.opt.WriteTimeout)
	return tx._cn
}

// TODO(vmihailenco): track and close prepared statements
func (tx *Tx) Prepare(q string) (*Stmt, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()
	return prepare(tx.db, cn, q)
}

func (tx *Tx) Exec(q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()

	res, err := simpleQuery(cn, q, args...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

func (tx *Tx) ExecOne(q string, args ...interface{}) (*Result, error) {
	res, err := tx.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

func (tx *Tx) Query(coll Collection, q string, args ...interface{}) (*Result, error) {
	if tx.done {
		return nil, errTxDone
	}

	cn := tx.conn()
	res, err := simpleQueryData(cn, coll, q, args...)
	if err != nil {
		tx.setErr(err)
		return nil, err
	}
	return res, nil
}

func (tx *Tx) Rower(row interface{}, cb func(), q string, args ...interface{}) error {
	if tx.done {
		return errTxDone
	}

	cn := tx.conn()

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		tx.setErr(err)
		return err
	}

	if err := cn.Flush(); err != nil {
		tx.setErr(err)
		return err
	}

	err := func() (e error) {
		var columns []string
		for {
			c, msgLen, err := cn.ReadMsgType()
			if err != nil {
				return err
			}
			switch c {
			case rowDescriptionMsg:
				columns, err = readRowDescription(cn)
				if err != nil {
					return err
				}
			case dataRowMsg:
				if err := readDataRow(cn, row, columns); err != nil {
					e = err
				} else {
					cb()
				}
			case commandCompleteMsg:
				b, err := cn.ReadN(msgLen)
				if err != nil {
					return err
				}
				//res = newResult(b)
				_ = b
			case readyForQueryMsg:
				_, err := cn.ReadN(msgLen)
				if err != nil {
					return err
				}
				return nil
			case errorResponseMsg:
				var err error
				e, err = cn.ReadError()
				if err != nil {
					return err
				}
			case noticeResponseMsg:
				if err := logNotice(cn, msgLen); err != nil {
					return err
				}
			case parameterStatusMsg:
				if err := logParameterStatus(cn, msgLen); err != nil {
					return err
				}
			default:
				if e != nil {
					return e
				}
				return fmt.Errorf("pg: Rower: unexpected message %#x", c)
			}
		}
	}()

	if err != nil {
		tx.setErr(err)
		return err
	}

	return nil
}

func (tx *Tx) QueryOne(record interface{}, q string, args ...interface{}) (*Result, error) {
	res, err := tx.Query(&singleRecordCollection{record}, q, args...)
	if err != nil {
		return nil, err
	}
	return assertOneAffected(res)
}

func (tx *Tx) Commit() error {
	if tx.done {
		return errTxDone
	}
	_, err := tx.Exec("COMMIT")
	if err != nil {
		tx.setErr(err)
	}
	tx.close()
	return err
}

func (tx *Tx) Rollback() error {
	if tx.done {
		return errTxDone
	}
	_, err := tx.Exec("ROLLBACK")
	if err != nil {
		tx.setErr(err)
	}
	tx.close()
	return err
}

func (tx *Tx) setErr(e error) {
	tx.err = e
}

func (tx *Tx) close() error {
	if tx.done {
		return nil
	}
	tx.done = true
	return tx.db.freeConn(tx._cn, tx.err)
}

func txFinalizer(tx *Tx) {
	if !tx.done {
		glog.Errorf("transaction was neither commited or rollbacked")
	}
}
