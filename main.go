package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	FedoraBroker = "amqps://fedora:@rabbitmq.fedoraproject.org/%2Fpublic_pubsub"
	Exchange     = "amq.topic"
	RoutingKey   = "org.fedoraproject.prod.copr.build.end"

	Owner        = "cgrates"
	MasterBranch = "master"
	//cacert and key paths
	CaCert = "/etc/fedora-messaging/cacert.pem"
	Cert   = "/etc/fedora-messaging/fedora-cert.pem"
	Key    = "/etc/fedora-messaging/fedora-key.pem"

	DownloadUrl = "https://download.copr.fedorainfracloud.org/results/"
	CGRPrefix   = "cgrates-"
	CGRSuffix   = "-cgrates"
	RpmSuffix   = "rpm"
	ArchBuild   = "x86_64"
	Current     = "cgrates-current"
	PackageDir  = "/var/packages/rpm"
	PkgOwner    = "owner"
)

type CoprBuild struct {
	Build   int    `json:"build"`
	Chroot  string `json:"chroot"`
	Copr    string `json:"copr"`
	Owner   string `json:"owner"`
	Pkg     string `json:"pkg"`
	Status  int    `json:"status"`
	User    string `json:"user"`
	Version string `json:"version"`
}

func newUuid() string {
	queueUUID := uuid.New()
	return queueUUID.String()
}

func setupTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(Cert, Key)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(CaCert)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	return tlsConfig, nil
}

func setupConn(tls *tls.Config) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.DialTLS_ExternalAuth(FedoraBroker, tls)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, err
}

func queueBind(ch *amqp.Channel) (string, error) {
	queue, err := ch.QueueDeclare(
		newUuid(),
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {

		return "", err
	}
	if err = ch.QueueBind(
		queue.Name,
		RoutingKey,
		Exchange,
		false,
		nil,
	); err != nil {
		return "", err
	}
	return queue.Name, nil
}

func consumeMessage(ctx context.Context, ch *amqp.Channel, queueName string) {
	errChan := make(chan error, 1)
	fileChan := make(chan string, 1)

	msgs, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println("Error consuming messages:", err)
		return
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping message consumption due to context cancellation")
			return
		case err := <-errChan:
			log.Println("Error:", err)
		case file := <-fileChan:
			log.Println("File created:", file)
		case <-ctx.Done():
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			go processMessage(errChan, fileChan, msg)
			msg.Ack(true)
		}
	}
}

func processMessage(errCh chan<- error, filech chan<- string, msg amqp.Delivery) {
	var coprBuild CoprBuild
	iter := jsoniter.ParseBytes(jsoniter.ConfigCompatibleWithStandardLibrary, msg.Body)

	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		if field == PkgOwner {
			if owner := iter.ReadString(); owner != Owner {
				return
			}
			break
		}
		iter.Skip()
	}

	if err := json.Unmarshal(msg.Body, &coprBuild); err != nil {
		errCh <- err
		return
	}

	if coprBuild.Version != "" {
		file, err := generateFiles(coprBuild)
		if err != nil {
			errCh <- err
			return
		}
		filech <- file
	}

}
func generateFiles(c CoprBuild) (file string, err error) {
	urlPath, err := url.JoinPath(DownloadUrl, c.Owner, c.Copr, c.Chroot, fmt.Sprintf("0%v", c.Build)+CGRSuffix, CGRPrefix+strings.Join([]string{c.Version, ArchBuild, RpmSuffix}, "."))
	if err != nil {
		return
	}
	file, err = downloadFile(strings.Join([]string{c.Version, ArchBuild, RpmSuffix}, "."), c.Copr, c.Chroot, urlPath)
	if err != nil {
		return
	}
	return
}

func downloadFile(fileName, projectName, chroot, url string) (filePath string, err error) {
	var (
		resp *http.Response
		file *os.File
	)
	resp, err = http.Get(url)
	if err != nil {
		return
	}
	log.Printf("Making a Request on %v\n", url)
	defer resp.Body.Close()
	if projectName == MasterBranch {
		projectName = "nightly"
	}
	dirPath := filepath.Join(PackageDir, projectName, chroot)
	if _, err = os.Stat(dirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, 0775); err != nil {
			return
		}
	}

	curr := filepath.Join(dirPath, strings.Join([]string{Current, RpmSuffix}, "."))
	if err = os.Remove(curr); err != nil && !os.IsNotExist(err) {
		return
	}

	filePath = filepath.Join(dirPath, CGRPrefix+fileName)
	if file, err = os.Create(filePath); err != nil {
		return
	}
	defer file.Close()
	if _, err = io.Copy(file, resp.Body); err != nil {
		return
	}
	err = os.Symlink(filePath, curr)
	if err != nil {
		log.Fatalf("Failed to create symlink: %s", err)
	}
	return
}

func main() {
	logName := flag.String("log_name", "", "Logger file name ")
	flag.Parse()

	logwriter, err := syslog.New(syslog.LOG_NOTICE, *logName)
	if err != nil {
		log.Fatal("Failed to initialize syslog writer: ", err)
	}
	log.SetOutput(logwriter)

	tlsConfig, err := setupTLS()
	if err != nil {
		log.Fatal(err)
	}

	conn, ch, err := setupConn(tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()
	defer ch.Close()

	queueName, err := queueBind(ch)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go consumeMessage(ctx, ch, queueName)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	log.Println("FecoPack started running...")
	<-sigs
	cancel()
	log.Println("Connections closed. Exiting...")
}
