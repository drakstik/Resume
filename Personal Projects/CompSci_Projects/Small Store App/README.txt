In here you will find

BACK END CODE:
	-MySQL Schema
	-MySQL Script of the Schema

FRONT END CODE:
	-Self CheckOut Application: This application creates a new salesReceipt. The customer simply logs in, chooses the item, and the quantity he/she wishes to purchase. 
                                    The application produces a salesReceipt complete with the date, total and salesID, but produces an error if the cashier 
                                    inputs a purchase quantity that exceeds the item’s `shelved` quantity.


To Use the Self CheckOut Application you need to:
	- Open the SelfCheckOutAPP file. 
	- Click on Sign Up
	- Enter your info, Click done (This will CREATE a new staff member with your name and your chosen password)
	- Or you could just use Username: David Mberingabo, Password: 1, to login
	- Once logged in, you can start filling your shopping cart
	- Choose the goods you want to purchase
	- Choose the quantity
	- Finalize your salesReceipt (This will UPDATE the `shelved` attirbute in each of the sold goods

ERRORS:
	- "Wrong username and/or password" (The given username and password are not in our DB)
	- "assetName is out of stock" (This good you are trying to purchase is out of stock)
	- "There are only __ left of assetName" (Try purchasing less of the good)


LESSONS LEARNED:
	- Learned how to use basic html and javascript
	- Learned how to connect html to MySQL through Java
	- Learned how to coordinate with my partner as we tackled different parts of the project
	- Learned that you should ALWAYS save your files (learnt this the hard way!)
	- We quickly realized that most of the information on a balance sheet and income statement can be derived from even less information

FUTURE WORK:
	- We tried to make this DB as general as possible in order to be accessible to all kinds of stores that sell physical goods, like a convenience store.
	  With our schema and a fully fleshed out Db and applications, we would exponantially increase the store's efficiency and reduce man hours required to operate a small store.
	- We currently use a small portion of the DB in our Application, but we intend to fully flesh out the DB with more functionality like:
		- Displaying realtime balance sheets and income statements
		- Creating an inventory management application that track purchases made by the store, shipments and business partners
		- Displaying all unpaid/paid invoices and their deadlines
	- 