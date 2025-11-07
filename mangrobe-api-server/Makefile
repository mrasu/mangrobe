
generate:
	sea-orm-cli generate entity -o src/infrastructure/db/entity --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/up:
	cd migration && cargo run -- up --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/down:
	cd migration && cargo run -- down --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development

migrate/fresh:
	cd migration && cargo run -- fresh --database-url postgres://postgres:@127.0.0.1:5432/mangrobe-development
