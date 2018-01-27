

# r_scripts README


### File descriptions

I really only have the Amazon portion of the price scraping set up. For this to make the most sense, files should be read in this order:

1. **sfx_config.R** - this is where I'm loading in libraries I need for this project and where I'm reading in sensitive files with the credentials to my database and the keys to the various API's I'm using. Start here.
2. **sfx_asin_crawler_functions.R** - this is where I'm storing all of the user-defined functions I've written to make this a bit more organized. If you see a function called elsewhere and you don't know where it's coming from, it's more than likely in here somewhere.
3. **sfx_asin_crawler_workflow.R** - self-explanatory. This is the overall workflow for the process of crawling potential ASINs that I'll want to track the price of. It reads in the config and functions file previously mentioned. 


### Database tables so far (MySQL):

1. **asin_stage01** - Pretty much all ASINs that are returned from the amazon product advertising API will land in this table. It's a kill-and-fill table though, so no history is saved here. This is just where I'm landing data to inspect it and see if it's an ASIN I want to track or not.
2. **asin_fact01** - this is where I'm implementing type 2 slowly changing dimensions for each product's price. I'm using effective start/end date as well as a "is_current" column flag. I'm not implementing the slowly changing dimensions with native database tools, mainly because this is a proof of concept project right now and I don't want anything to be coupled too closely to a certain type of database. I'm using the most basic data types in my MySQL database because I don't want to use things that only work in MySQL. I mention this because it will appear as though I'm not implementing things correctly, but I'm really just trying to test this out.
3. **asin_list01** - this is where I'll keep just the ASIN, product title (maybe), and a flag (1/0) indicating whether this is a product type that I want to track in my asin_fact table mentioned above. Eventually, as I populate more of this field, I'm hoping to implement some very light machine learning algorithms that could detect potential ASINs in my stage table that I'd like tracked in my fact table to automate this manual step. Until I have this training data built up, this will remain a manual step which is fine. 




