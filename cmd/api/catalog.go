package main

import (
	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
)

type productJSON struct {
	SKU            string `json:"sku" db:"sku"`
	Name           string `json:"name" db:"name"`
	Type           string `json:"type" db:"type"`
	Price          int    `json:"price" db:"price"`
	Currency       string `json:"currency" db:"currency"`
	AvailableCount int    `json:"available_count" db:"available_count"`
}

func (a *app) catalog(c fiber.Ctx) error {
	limit := fiber.Query[int](c, "limit", 50)
	offset := fiber.Query[int](c, "offset", 0)
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	rows, err := a.pool.Query(c.Context(), `
		SELECT sku, name, type, price, currency, available_count
		  FROM products
		 ORDER BY available_count DESC, sku
		 LIMIT @limit OFFSET @offset
	`, pgx.NamedArgs{"limit": limit, "offset": offset})
	if err != nil {
		return err
	}
	list, err := pgx.CollectRows(rows, pgx.RowToStructByName[productJSON])
	if err != nil {
		return err
	}
	if list == nil {
		list = []productJSON{}
	}
	return c.JSON(fiber.Map{"products": list})
}
