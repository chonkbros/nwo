setup:
	@echo "Setting up the project..."
	python3 -m venv venv
	./venv/bin/pip install -r requirements.txt
	@echo "Setting up MongoDB with Docker..."
	docker pull mongodb/mongodb-community-server
	docker run --name mongo -p 27017:27017 -d mongodb/mongodb-community-server:latest
	@echo "MongoDB container is set up. Running containers:"
	docker container ls
	@echo "To access the MongoDB shell, run: docker exec -it mongo mongosh"
	@echo "In the MongoDB shell, you can run MongoDB commands like db.runCommand({hello: 1})"
	@echo "Scheduling the orchestration script..."
	@echo "Please run this command to add to cron:"
	@echo "0 3 * * 1 /path/to/python /path/to/orchestration.py"

.PHONY: setup
