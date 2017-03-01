package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	collectd "github.com/paulhammond/gocollectd"
)

const (
	appName             = "influxdb-collectd-proxy"
	influxWriteInterval = time.Second
	influxWriteLimit    = 50
	influxDbPassword    = ""
	influxDbUsername    = ""
	influxDbName        = "collectd_proxy"
)

var (
	proxyHost   *string
	proxyPort   *string
	typesdbPath *string
	logPath     *string
	verbose     *bool

	// influxdb options
	host       *string
	username   *string
	password   *string
	database   *string
	normalize  *bool
	storeRates *bool

	// Format
	hostnameAsColumn   *bool
	pluginnameAsColumn *bool

	types Types
	//ic    influxdb.NewHTTPClient
	ic          influxdb.Client
	beforeCache map[string]CacheEntry
)

/* point cache to perform data normalization for COUNTER and DERIVE types */

type CacheEntry struct {
	Timestamp int64
	Value     float64
	Hostname  string
}

// signal handler
func handleSignals(c chan os.Signal) {
	// block until a signal is received
	sig := <-c

	log.Printf("exit with a signal: %v\n", sig)
	os.Exit(1)
}

func getenvOrDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func init() {
	// log options
	log.SetPrefix("[" + appName + "] ")

	// proxy options
	proxyHost = flag.String("proxyhost", "0.0.0.0", "host for proxy")
	proxyPort = flag.String("proxyport", "8096", "port for proxy")
	typesdbPath = flag.String("typesdb", "types.db", "path to Collectd's types.db")
	logPath = flag.String("logfile", "", "path to log file (log to stderr if empty)")
	verbose = flag.Bool("verbose", false, "true if you need to trace the requests")

	// influxdb options
	host = flag.String("influxdb", "localhost:8086", "host:port for influxdb")
	username = flag.String("username", getenvOrDefault(influxDbUsername, "root"), "username for influxdb or $INFLUXDB_PROXY_USERNAME env")
	password = flag.String("password", getenvOrDefault(influxDbPassword, "root"), "password for influxdb or $INFLUXDB_PROXY_PASSWORD env")
	database = flag.String("database", getenvOrDefault(influxDbName, ""), "database for influxdb or $INFLUXDB_PROXY_DATABASE env")
	normalize = flag.Bool("normalize", true, "true if you need to normalize data for COUNTER types (over time)")
	storeRates = flag.Bool("storerates", true, "true if you need to derive rates from DERIVE types")

	// format options
	hostnameAsColumn = flag.Bool("hostname-as-column", false, "true if you want the hostname as column, not in series name")
	pluginnameAsColumn = flag.Bool("pluginname-as-column", false, "true if you want the plugin name as column")
	flag.Parse()

	beforeCache = make(map[string]CacheEntry)

	// read types.db
	var err error
	types, err = ParseTypesDB(*typesdbPath)
	if err != nil {
		log.Fatalf("failed to read types.db: %v\n", err)
	}
}

func main() {
	var err error

	if *logPath != "" {
		logFile, err := os.OpenFile(*logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("failed to open file: %v\n", err)
		}
		log.SetOutput(logFile)
		defer logFile.Close()
	}

	// make influxdb client

	// register a signal handler
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, os.Kill)
	go handleSignals(sc)

	// make channel for collectd
	c := make(chan collectd.Packet)

	// then start to listen
	go collectd.Listen(*proxyHost+":"+*proxyPort, c)
	log.Printf("proxy started on %s:%s\n", *proxyHost, *proxyPort)
	timer := time.Now()
	//var seriesGroup []*influxdb.Series
	ic, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     *host,
		Username: *username,
		Password: *password,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer ic.Close()

	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  *database,
		Precision: "ns",
	})
	if err != nil {
		log.Fatalln("Error: ", err)
	}

	cnt := 0
	for {
		/*~~~~~~~~~~~~~~~~~~~~~~~~~~ Process collect packet ~~~~~~~~~~~~~~~~~~~~~~~~~~*/
		packet := <-c
		if *verbose {

			log.Printf("[TRACE] got a packet: %v\n", packet)
		}
		// for all metrics in the packet

		for i, _ := range packet.ValueNames() {
			values, _ := packet.ValueNumbers()
			cnt++
			// get a type for this packet
			t := types[packet.Type]

			// pass the unknowns
			if t == nil && packet.TypeInstance == "" {
				log.Printf("unknown type instance on %s\n", packet.Plugin)
				continue
			}
			// as hostname contains commas, let's replace them
			hostName := strings.Replace(packet.Hostname, ".", "_", -1)
			// if there's a PluginInstance, use it
			pluginName := packet.Plugin
			if packet.PluginInstance != "" {
				pluginName += "-" + packet.PluginInstance
			}

			// if there's a TypeInstance, use it
			typeName := packet.Type
			if packet.TypeInstance != "" {
				typeName += "-" + packet.TypeInstance
			} else if t != nil {
				typeName += "-" + t[i][0]
			}

			// Append "-rx" or "-tx" for Plugin:Interface - by linyanzhong
			if packet.Plugin == "interface" {
				if i == 0 {
					typeName += "-tx"
				} else if i == 1 {
					typeName += "-rx"
				}
			}

			name := hostName + "." + pluginName + "." + typeName

			//name := fmt.Sprintf("%s_%s", packet.Identifier.Plugin, packet.DSName(i))

			nameNoHostname := pluginName + "." + typeName
			// influxdb stuffs
			timestamp := packet.Time().UnixNano() / 1000000
			value := values[i].Float64()
			dataType := packet.DataTypes[i]
			var kind string
			kind = types[packet.Type][i][0]
			readyToSend := true
			normalizedValue := value

			if *normalize && dataType == collectd.TypeCounter || *storeRates && dataType == collectd.TypeDerive {
				if before, ok := beforeCache[name]; ok && before.Value != math.NaN() {
					// normalize over time
					if timestamp-before.Timestamp > 0 {
						normalizedValue = (value - before.Value) / float64((timestamp-before.Timestamp)/1000)
					} else {
						normalizedValue = value - before.Value
					}
				} else {
					// skip current data if there's no initial entry
					readyToSend = false
				}
				entry := CacheEntry{
					Timestamp: timestamp,
					Value:     value,
					Hostname:  hostName,
				}
				beforeCache[name] = entry
			}

			if readyToSend {
				columns := []string{"time", "value"}
				points_values := []interface{}{timestamp, normalizedValue}
				name_value := name

				// option hostname-as-column is true
				if *hostnameAsColumn {
					name_value = nameNoHostname
					columns = append(columns, "hostname")
					points_values = append(points_values, hostName)
				}

				// option pluginname-as-column is true
				if *pluginnameAsColumn {
					columns = append(columns, "plugin")
					points_values = append(points_values, pluginName)
				}
				tags := make(map[string]string)
				fields := make(map[string]interface{})
				var tsname string
				//if packet.Type != "" {
				if kind != "" {
					tsname = fmt.Sprintf("%s_%s", packet.Plugin, kind)
				} else {
					tsname = fmt.Sprintf("%s_value", packet.Plugin)
				}
				//tsname = name_value
				//dataType
				// Convert interface back to actual type, then to float64
				switch packet.DataTypes[i] {
				case collectd.TypeGauge:
					fields["value"] = float64(normalizedValue)
				case collectd.TypeDerive:
					fields["value"] = float64(normalizedValue)
				case collectd.TypeCounter:
					fields["value"] = float64(normalizedValue)
				}

				if hostName != "" {
					tags["host"] = packet.Hostname
				}
				if packet.PluginInstance != "" {
					tags["instance"] = packet.PluginInstance
				}
				if packet.Type != "" {
					tags["type"] = packet.Type
				}
				if packet.TypeInstance != "" {
					tags["type_instance"] = packet.TypeInstance
				}
				log.Printf("name_value= %s AND  NAME :%s", name_value, tsname)
				// Create Influx Point
				pt, err := influxdb.NewPoint(
					//name_value,
					tsname,
					tags,
					//map[string]interface{}{"value": normalizedValue,},
					fields,
					packet.Time(),
				)
				if err != nil {
					log.Printf("Error:", err.Error())
					continue
				}
				bp.AddPoint(pt)

				if *verbose {
					log.Printf("[TRACE] ready to send series: %v\n", cnt)
				}

			}
		}

		/*~~~~~~~~~~~~~~~~~~~~~~~~~~  End Process packet ~~~~~~~~~~~~~~~~~~~~~~~~~~*/

		if time.Since(timer) < influxWriteInterval && cnt < influxWriteLimit {
			continue
		} else {
			if cnt > 0 {

				err = ic.Write(bp)
				if err != nil {
					log.Printf("failed to write series group to influxdb: %s\n", err)
				}
				if *verbose {
					log.Printf("[TRACE] wrote %d series\n", cnt)

				}
				cnt = 0
				bp, err = influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
					Database:  *database,
					Precision: "ns",
				})
				if err != nil {
					log.Fatalln("Error: ", err)
				}
			}
			timer = time.Now()
		}
	}
}
