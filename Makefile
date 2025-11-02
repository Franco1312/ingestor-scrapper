.PHONY: help crawl test

# Default spider (can be overridden)
SPIDER ?= bcra

help: ## Mostrar ayuda
	@echo "Uso: make [comando] [SPIDER=bcra]"
	@echo ""
	@echo "Comandos:"
	@echo "  make crawl          - Ejecuta spider 'bcra'"
	@echo "  make test           - Ejecuta todos los tests"
	@echo ""
	@echo "Ejemplos:"
	@echo "  make crawl          - Ejecuta spider 'bcra'"
	@echo "  make crawl bcra     - Ejecuta spider 'bcra'"
	@echo "  make crawl SPIDER=universal - Ejecuta spider 'universal'"
	@echo "  make test           - Ejecuta tests con pytest"

crawl: ## Ejecutar spider de Scrapy
	scrapy crawl $(SPIDER)

test: ## Ejecutar tests
	pytest ingestor_scrapper/ -v

test-cov: ## Ejecutar tests con cobertura
	pytest ingestor_scrapper/ -v --cov=ingestor_scrapper.adapters.parsers.bcra_excel --cov=ingestor_scrapper.adapters.normalizers.bcra_monetario --cov-report=term-missing

