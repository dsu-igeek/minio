module github.com/minio/minio

go 1.13

require (
	cloud.google.com/go v0.57.0 // indirect
	cloud.google.com/go/storage v1.6.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-storage-blob-go v0.8.0
	github.com/Shopify/sarama v1.24.1
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/alecthomas/participle v0.2.1
	github.com/aws/aws-sdk-go v1.36.3
	github.com/bcicen/jstream v0.0.0-20190220045926-16c1f8af81c2
	github.com/beevik/ntp v0.2.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/cheggaaa/pb v1.0.28
	github.com/coredns/coredns v1.4.0
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/djherbis/atime v1.0.0
	github.com/dsu-igeek/astrolabe-demo v0.0.0-00010101000000-000000000000
	github.com/dustin/go-humanize v1.0.0
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/elazarl/go-bindata-assetfs v1.0.0
	github.com/fatih/color v1.10.0
	github.com/fatih/structs v1.1.0
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.1.2
	github.com/gopherjs/gopherjs v0.0.0-20190328170749-bb2674552d8f // indirect
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.4
	github.com/gorilla/rpc v1.2.0+incompatible
	github.com/hashicorp/raft v1.1.1-0.20190703171940-f639636d18e0 // indirect
	github.com/hashicorp/vault/api v1.0.4
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/json-iterator/go v1.1.10
	github.com/klauspost/compress v1.10.10
	github.com/klauspost/cpuid v1.2.4
	github.com/klauspost/pgzip v1.2.4
	github.com/klauspost/readahead v1.3.1
	github.com/klauspost/reedsolomon v1.9.7
	github.com/kurin/blazer v0.5.4-0.20200327014341-8f90a40f8af7
	github.com/lib/pq v1.9.0
	github.com/mattn/go-colorable v0.1.8
	github.com/mattn/go-isatty v0.0.12
	github.com/mattn/go-runewidth v0.0.4 // indirect
	github.com/miekg/dns v1.1.8
	github.com/minio/cli v1.22.0
	github.com/minio/gokrb5/v7 v7.2.5
	github.com/minio/hdfs/v3 v3.0.1
	github.com/minio/highwayhash v1.0.0
	github.com/minio/lsync v1.0.1
	github.com/minio/minio-go/v6 v6.0.55
	github.com/minio/parquet-go v0.0.0-20200414234858-838cfa8aae61
	github.com/minio/sha256-simd v0.1.1
	github.com/minio/simdjson-go v0.1.5-0.20200303142138-b17fe061ea37
	github.com/minio/sio v0.2.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/montanaflynn/stats v0.5.0
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2 // indirect
	github.com/nats-io/go-nats-streaming v0.4.4 // indirect
	github.com/nats-io/nats-server v1.4.1 // indirect
	github.com/nats-io/nats-server/v2 v2.1.2
	github.com/nats-io/nats-streaming-server v0.14.2 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/stan.go v0.4.5
	github.com/ncw/directio v1.0.5
	github.com/nsqio/go-nsq v1.0.7
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/rcrowley/go-metrics v0.0.0-20190704165056-9c2d0518ed81 // indirect
	github.com/rjeczalik/notify v0.9.2
	github.com/rs/cors v1.6.0
	github.com/secure-io/sio-go v0.3.0
	github.com/shirou/gopsutil v2.20.3-0.20200314133625-53cec6b37e6a+incompatible
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/assertions v0.0.0-20190401211740-f487f9de1cd3 // indirect
	github.com/streadway/amqp v0.0.0-20190404075320-75d898a42a94
	github.com/tinylib/msgp v1.1.2
	github.com/valyala/tcplisten v0.0.0-20161114210144-ceec8f93295a
	github.com/vmware-tanzu/astrolabe v0.4.0
	github.com/vmware-tanzu/astrolabe-velero v0.0.0-00010101000000-000000000000
	github.com/vmware-tanzu/velero-plugin-for-aws v0.0.0-00010101000000-000000000000
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.uber.org/atomic v1.6.0
	golang.org/x/crypto v0.0.0-20201203163018-be400aefbc4c
	golang.org/x/net v0.0.0-20201110031124-69a78807bb2b
	golang.org/x/sys v0.0.0-20210112080510-489259a85091
	google.golang.org/api v0.25.0
	gopkg.in/cheggaaa/pb.v1 v1.0.28 // indirect
	gopkg.in/ldap.v3 v3.0.3
	gopkg.in/olivere/elastic.v5 v5.0.80
	gopkg.in/yaml.v2 v2.4.0
)

// Added for go1.13 migration https://github.com/golang/go/issues/32805
replace github.com/gorilla/rpc v1.2.0+incompatible => github.com/gorilla/rpc v1.2.0

// Allow this for offline builds
replace github.com/eapache/go-xerial-snappy => github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21

replace github.com/eapache/queue => github.com/eapache/queue v1.1.0

replace github.com/mattn/go-runewidth => github.com/mattn/go-runewidth v0.0.4

replace github.com/mitchellh/mapstructure => github.com/mitchellh/mapstructure v1.1.2

// Version 1.2.0 adds support for go modules
replace github.com/hashicorp/vault => github.com/hashicorp/vault v1.2.0-beta2

replace github.com/vmware-tanzu/astrolabe => ../../vmware-tanzu/astrolabe

replace github.com/vmware-tanzu/velero => ../../vmware-tanzu/velero

replace google.golang.org/grpc => google.golang.org/grpc v1.29.0

replace github.com/dsu-igeek/astrolabe-demo => ../../dsu-igeek/astrolabe-demo

replace github.com/vmware-tanzu/velero-plugin-for-aws => ../../vmware-tanzu/velero-plugin-for-aws

replace github.com/vmware-tanzu/velero-plugin-for-vsphere => ../../vmware-tanzu/velero-plugin-for-vsphere

replace github.com/vmware-tanzu/astrolabe-velero => ../../vmware-tanzu/astrolabe-velero
