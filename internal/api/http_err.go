package api

import (
	"errors"

	"github.com/labstack/echo/v4"
)

func Wrap(err error, httpStatus int, errMsg string) *echo.HTTPError {
	respError := echo.HTTPError{Code: httpStatus, Message: errMsg, Internal: err}
	return &respError
}

func AsHttpErrOrWrap(err error, httpStatus int, errMsg string) *echo.HTTPError {
	var respError *echo.HTTPError
	if errors.As(err, &respError) {
		return respError
	}
	return Wrap(err, httpStatus, errMsg)
}
