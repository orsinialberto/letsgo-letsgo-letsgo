package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

type customer struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

func main() {
	router := gin.Default()

	router.POST("/customers", postCustomers)

	router.Run("localhost:8080")
}

func postCustomers(c *gin.Context) {
	var newCustomer customer

	// Call BindJSON to bind the received JSON to newCustomer.
	if err := c.BindJSON(&newCustomer); err != nil {
		return
	}

	log.Printf("creating customer %s", newCustomer.Email)

	c.IndentedJSON(http.StatusCreated, newCustomer)
}
