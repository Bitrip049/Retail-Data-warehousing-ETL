#!/usr/bin/env python
import snowflake.connector

# Gets the version
ctx= snowflake.connector.connect(
    user='bitrip_p',
    password='Hesoyam4471404',
    account='ye70701.south-central-us.azure',
    role='accountadmin',
    warehouse='training'

    )
cs = ctx.cursor()



