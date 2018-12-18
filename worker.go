package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	fmt.Println("Start of test")

	arrClientIps := readDeployments()

	fmt.Println(arrClientIps)

	noOfWorker := len(arrClientIps)

	done := make(chan bool, noOfWorker)

	for i := 0; i < noOfWorker; i++ {
		go worker(i, arrClientIps[i], done)
	}

	for i := 0; i < noOfWorker; i++ {
		<-done
	}
	fmt.Println("End of test")
}

func readDeployments() []string {

	file, err := os.Open("file.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	arrayClientIPs := make([]string, 0)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		deploymentName := scanner.Text()
		if strings.HasPrefix(deploymentName, "service-fabrik") {
			guid := deploymentName[20:len(deploymentName)]
			clientIP := "pg-" + guid + ".psql-awsmaz.sapcloud.io."
			arrayClientIPs = append(arrayClientIPs, clientIP)
		} else {
			fmt.Println("Not a valid deployment")
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return arrayClientIPs
}

func worker(workerNumer int, clientIP string, done chan bool) {
	var status bool
	var stopTime time.Time
	status = true
	for true {
		mode, noError := getPGMode(clientIP)
		if noError {
			if !mode {
				fmt.Println("I am worker number : ", workerNumer, "connecting to :", clientIP, "--> MASTER NODE")
				if !status {
					diffTime := time.Now().Sub(stopTime)
					fmt.Println("Downtime is --> ", diffTime)
				}
				status = true
				stopTime = time.Now()
			} else {
				fmt.Println("I am worker number : ", workerNumer, "connecting to :", clientIP, "--> SLAVE NODE")
				status = false
			}
		} else {
			fmt.Println("I am worker number : ", workerNumer, "connecting to :", clientIP, "--> ERROR")
			if status {
				stopTime = time.Now()
			}
			status = false
		}
		time.Sleep(1 * time.Second)
	}
	//done <- true
}

func getPGMode(host string) (bool, bool) {

	var connectionString string
	var identifiedPGMode bool

	connectionString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, 5432, "vcap", "vcap", "postgres")
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Println("Error occurred while establishing a connection with postgres.", err.Error())
		return false, false
	}
	defer db.Close()

	rows, err := db.Query("select pg_is_in_recovery()")
	if err != nil {
		log.Println("Error occurred while executing select pg_is_in_recovery().", err.Error())
		return false, false
	}
	for rows.Next() {
		err := rows.Scan(&identifiedPGMode)
		if err != nil {
			log.Println("Error occurred while executing select pg_is_in_recovery().", err.Error())
			return false, false
		}
	}
	return identifiedPGMode, true

}
