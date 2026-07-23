//go:build temporalstore_direct

package main

import (
	"fmt"
	"log"

	temporalstore "temporalstore"
)

func main() {
	client, err := temporalstore.Connect(temporalstore.Options{
		MetaserverAddr: "127.0.0.1:18200",
		NamespaceName:  "sdk_ns",
		TableName:      "sdk_table",
		PSM:            "temporalstore.go.example",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.PutString("go:user:42", `{"tier":"gold"}`); err != nil {
		log.Fatal(err)
	}
	profile, err := client.GetString("go:user:42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("profile=%s\n", profile)
	if err := client.Expire("go:user:42", 60000); err != nil {
		log.Fatal(err)
	}
	ttlMs, err := client.TTL("go:user:42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ttl_ms=%d\n", ttlMs)

	if err := client.HSet("go:user:42:features", "ctr_7d", "0.042"); err != nil {
		log.Fatal(err)
	}
	ctr, err := client.HGet("go:user:42:features", "ctr_7d")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ctr_7d=%s\n", ctr)

	if err := client.SAdd("go:user:42:campaigns", "campaign_100"); err != nil {
		log.Fatal(err)
	}
	campaigns, err := client.SMembers("go:user:42:campaigns")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("campaigns=%+v\n", campaigns)

	key := "go:user:42:sequence"
	err = client.AddSequenceFeatureRows(key, []temporalstore.SequenceFeatureRow{
		{Timestamp: 1700000000000, GID: 900, ActionType: 1, Duration: 31, AuthorID: 7000},
		{Timestamp: 1700000001000, GID: 901, ActionType: 3, Duration: 120, AuthorID: 7001},
	})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := client.QuerySequenceFeatureRows(
		key,
		1700000000000,
		1700000002000,
		10,
		[]temporalstore.FeatureFilter{
			{Field: "action_type", Op: temporalstore.FeatureFilterEqual, Value: 3},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%+v\n", rows)

	riskKey := "go:user:42:risk"
	if err := client.RiskIncrement(riskKey, 1, 24*3600, temporalstore.RiskOneMinute, "go-risk-1", 0); err != nil {
		log.Fatal(err)
	}
	riskCount, err := client.RiskCount(riskKey, temporalstore.RiskOneMinute, -1, 0, temporalstore.WindowHour)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("risk_count=%d\n", riskCount)
}
