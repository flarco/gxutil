package gxutil

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	color "github.com/fatih/color"
	"github.com/flarco/stacktrace"
	gomail "gopkg.in/gomail.v2"
)

var (
	// SMTPServer is email SMTP server host
	SMTPServer = "smtp.gmail.com"

	// SMTPPort is email SMTP server port
	SMTPPort = 465

	// SMTPUser is SMTP user name
	SMTPUser = os.Getenv("SMTP_USER")

	// SMTPPass is user password
	SMTPPass = os.Getenv("SMTP_PASS")

	// AlertEmail is the email address to send errors to
	AlertEmail = os.Getenv("ALERT_EMAIL")
)

const (
	alphaRunes        = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	aplhanumericRunes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
)

// GetType : return the type of an interface
func GetType(myvar interface{}) string {
	t := reflect.TypeOf(myvar)
	if t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	}
	return t.Name()
}

// F : fmt.Sprintf
func F(format string, args ...interface{}) string {
	return fmt.Sprintf(format, args...)
}

// mapMerge copies key values from `mS` into `mT`
func mapMerge(mT map[string]interface{}, mS map[string]interface{}) map[string]interface{} {
	for key, val := range mS {
		mT[key] = val
	}
	return mT
}

// R : Replacer
// R("File {file} had error {error}", "file", file, "error", err)
func R(format string, args ...string) string {
	args2 := make([]string, len(args))
	for i, v := range args {
		if i%2 == 0 {
			args2[i] = fmt.Sprintf("{%v}", v)
		} else {
			args2[i] = fmt.Sprint(v)
		}
	}
	r := strings.NewReplacer(args2...)
	return r.Replace(format)
}

// Rm is like R, for replacing with a map
func Rm(format string, m map[string]interface{}) string {
	args, i := make([]string, len(m)*2), 0
	for k, v := range m {
		args[i] = "{" + k + "}"
		args[i+1] = fmt.Sprint(v)
		i += 2
	}
	return strings.NewReplacer(args...).Replace(format)
}

// PrintV prints the value of object
func PrintV(v interface{}) {
	println(F("%#v", v))
}

// PrintT prints the type of object
func PrintT(v interface{}) {
	println(F("%T", v))
}

// Propagate is a modified version of stacktrace Propagate
func Propagate(err error, msg string) error {
	return stacktrace.Propagate(err, msg, 4)
}

// IsErr : checks for error
func IsErr(err error, msg string) bool {
	if err != nil {
		LogError(stacktrace.Propagate(err, msg, 3))
		return true
	}
	return false
}

func isErrP(err error, msg string, callerSkip int) bool {
	if err != nil {
		LogError(stacktrace.Propagate(err, msg, callerSkip))
		return true
	}
	return false
}

// IsErrExit : check for err and exits if error
func IsErrExit(err error, msg string) {
	if err != nil {
		LogErrorExit(stacktrace.Propagate(err, msg, 3))
	}
}

// Now : Get unix epoch time in milli
func Now() int64 {
	return int64(time.Now().UnixNano() / 1000000)
}

func uintStr(val string) uint {
	val64, err := strconv.ParseUint(val, 10, 32)
	isErrP(err, "Failed to ParseUint", 4)
	return uint(val64)
}

func strUint(val uint) string {
	return fmt.Sprint(val) //uint to string
}

func strInt(val int) string {
	return strconv.Itoa(val) // int to string
}

func strInt64(val int64) string {
	return strconv.FormatInt(val, 10) // int64 to string
}

func strFloat64(val float64) string {
	return strconv.FormatFloat(val, 'f', 6, 64) // float64 to string
}

// Log : print text
func Log(text string) {
	fmt.Fprintf(os.Stderr, "%s -- %s\n", time.Now().Format("2006-01-02 15:04:05"), text)
}

// LogC : print text in specified color
func LogC(text string, col string, w io.Writer) {
	var textColored string
	timeColored := color.CyanString(time.Now().Format("2006-01-02 15:04:05"))
	switch col {
	case "red":
		textColored = color.RedString(text)
	case "green":
		textColored = color.GreenString(text)
	case "blue":
		textColored = color.BlueString(text)
	case "magenta":
		textColored = color.MagentaString(text)
	case "white":
		textColored = color.WhiteString(text)
	case "cyan":
		textColored = color.CyanString(text)
	default:
		textColored = text
	}
	// fmt.Println(fmt.Sprintf("%s -- %s", timeColored, textColored))
	fmt.Fprintf(w, "%s -- %s\n", timeColored, textColored)
}

// LogCGreen prints in green
func LogCGreen(text string) { LogC(text, "green", os.Stderr) }

// LogCRed prints in red
func LogCRed(text string) { LogC(text, "red", os.Stderr) }

// LogCRedErr prints in red to Stderr
func LogCRedErr(text string) { LogC(text, "red", os.Stderr) }

// LogCBlue prints in blue
func LogCBlue(text string) { LogC(text, "blue", os.Stderr) }

// LogCMagenta print in magenta
func LogCMagenta(text string) { LogC(text, "magenta", os.Stderr) }

// LogCWhite prints in white
func LogCWhite(text string) { LogC(text, "white", os.Stderr) }

// LogCCyan prints in white
func LogCCyan(text string) { LogC(text, "cyan", os.Stderr) }

// LogError handles logging of an error, useful for reporting
func LogError(E error) {
	if E != nil {
		LogCRedErr(Propagate(E, E.Error()).Error())
	}
}

// LogErrorExit handles logging of an error and exits, useful for reporting
func LogErrorExit(E error) {
	if E != nil {
		LogCRedErr(E.Error())
		log.Fatal(Propagate(E, E.Error()))
	}
}

// LogErrorMail handles logging of an error and mail it to self
func LogErrorMail(E error) {
	LogCRedErr(E.Error())
	SendMail(SMTPUser, []string{AlertEmail}, "Error | "+os.Args[0], E.Error())
}

// LogIfError handles logging of an error if it i not nil, useful for reporting
func LogIfError(E error) {
	if E != nil {
		LogError(E)
	}
}

// SendMail sends an email to the specific email address
// https://godoc.org/gopkg.in/gomail.v2#example-package
func SendMail(from string, to []string, subject string, textHTML string) error {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", to...)
	// m.SetAddressHeader("Cc", "dan@example.com", "Dan")
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", textHTML)
	// m.Attach("/home/Alex/lolcat.jpg")

	d := gomail.NewDialer(SMTPServer, SMTPPort, SMTPUser, SMTPPass)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	// Send the email
	err := d.DialAndSend(m)
	return err
}

// Check logs an error
func Check(e error, msg string) {
	if e != nil {
		println(Propagate(e, msg))
	}
}

// Panic panics on error
func Panic(e error, msg string) {
	if e != nil {
		panic(Propagate(e, msg))
	}
}

// Error returns stacktrace error with message
func Error(e error, msg string) error {
	return stacktrace.Propagate(e, msg, 3)
}

// Tee prints stream of text of reader
func Tee(reader io.Reader, limit int) io.Reader {
	pipeR, pipeW := io.Pipe()

	cnt := 0
	go func() {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			cnt++
			if cnt > limit {
				break
			}
			bytes := scanner.Bytes()
			nl := []byte("\n")
			fmt.Println(string(bytes))
			pipeW.Write(append(bytes, nl...))
		}
		pipeW.Close()
	}()

	return pipeR
}

// RandString returns a random string of len n with the provided char set
// charset can be `aplha` or `aplhanumeric`
func RandString(charset string, n int) string {
	b := make([]byte, n)
	letterBytes := alphaRunes
	if charset == "aplhanumeric" {
		letterBytes = aplhanumericRunes
	} 

	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	
	return string(b)
}
