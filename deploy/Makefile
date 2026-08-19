.PHONY: up down reset clean logs

# Starts the lab in the background (persists data from previous runs)
up:
	docker-compose up -d --build

# Stops the lab but KEEPS the data
down:
	docker-compose down

# Full reset: stops, DESTROYS all data, and starts from scratch
reset:
	@echo "🛑 Stopping services and destroying incident data..."
	docker-compose down -v
	@echo "🧹 Cleanup complete. Database, Kafka, and metrics are wiped clean!"
	@echo "🚀 Booting up the lab again..."
	docker-compose up -d --build

# Only destroys the data (without restarting)
clean:
	@echo "🛑 Destroying all data volumes..."
	docker-compose down -v