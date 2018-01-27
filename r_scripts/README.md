

# r_scripts README


### File descriptions

I really only have the Amazon portion of the price scraping set up. For this to make the most sense, files should be read in this order:

1. **sfx_config.R** - this is where I'm loading in libraries I need for this project and where I'm reading in sensitive files with the credentials to my database and the keys to the various API's I'm using. Start here.
2. **sfx_asin_crawler_functions.R** - this is where I'm storing all of the user-defined functions I've written to make this a bit more organized. If you see a function called elsewhere and you don't know where it's coming from, it's more than likely in here somewhere.
3. **sfx_asin_crawler_workflow.R** - self-explanatory. This is the overall workflow for the process of crawling potential ASINs that I'll want to track the price of. It reads in the config and functions file previously mentioned. 





