package main

// cmd/worker is the FlowForge worker entry point.
// It connects to PostgreSQL and Redis, joins the consumer group,
// and processes jobs from the Redis stream.
