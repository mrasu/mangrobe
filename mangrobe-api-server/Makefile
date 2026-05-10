
generate:
	sea-orm-cli generate entity -o src/infrastructure/db/entity --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/up:
	cargo run --bin migration -- up --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/down:
	cargo run --bin migration -- down --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/fresh:
	cargo run --bin migration -- fresh --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development
