
generate:
	sea-orm-cli generate entity -o src/infrastructure/db/entity --database-url postgres://postgres:@127.0.0.1:5432/mangroove-development

migrate/up:
	cd migration && cargo run -- up

migrate/down:
	cd migration && cargo run -- down
