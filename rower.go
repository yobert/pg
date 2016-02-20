package pg

import (
	"fmt"
	"reflect"
)

func (db *DB) Rower(q string, x ...interface{}) error {
	if len(x) == 0 {
		return errNoCallback
	}
	args := x[:len(x)-1]
	cb := x[len(x)-1]

	if cb == nil {
		return errNilCallback
	}

	cn, err := db.conn()
	if err != nil {
		return err
	}

	if err := writeQueryMsg(cn.buf, q, args...); err != nil {
		db.freeConn(cn, err)
		return err
	}

	if err := cn.Flush(); err != nil {
		db.freeConn(cn, err)
		return err
	}

	err = func() (e error) {
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
				loader := newCallbackLoader(reflect.ValueOf(cb))

				if err := readDataRow(cn, loader, columns); err != nil {
					return err
				} else {
					err = loader.Call()
					if err != nil {
						return err
					}
				}
			case commandCompleteMsg:
				_, err := cn.ReadN(msgLen)
				if err != nil {
					return err
				}
			case readyForQueryMsg:
				_, err := cn.ReadN(msgLen)
				if err != nil {
					return err
				}
				return e
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
		db.freeConn(cn, err)
		return err
	}

	db.freeConn(cn, nil)
	return nil
}

func (tx *Tx) Rower(q string, x ...interface{}) error {
	if len(x) == 0 {
		return errNoCallback
	}
	args := x[:len(x)-1]
	cb := x[len(x)-1]

	if cb == nil {
		return errNilCallback
	}

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
				loader := newCallbackLoader(reflect.ValueOf(cb))

				if err := readDataRow(cn, loader, columns); err != nil {
					return err
				} else {
					err = loader.Call()
					if err != nil {
						return err
					}
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
				return e
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
