test:
	docker-compose up -d
	cargo test -- --test-threads 1
	docker-compose down
