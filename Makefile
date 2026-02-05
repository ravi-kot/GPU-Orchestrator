.PHONY: up down logs scale load incident

up:
	cd infra && docker compose up --build -d

down:
	cd infra && docker compose down -v

logs:
	cd infra && docker compose logs -f --tail=200

scale:
	cd infra && docker compose up -d --scale worker=$(N)

load:
	python scripts/loadgen.py --n 300 --job-type cv_blur

incident:
	bash scripts/incident_replay.sh


