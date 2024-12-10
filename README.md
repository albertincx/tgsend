
Description
===========

*Tgsend* helps to send a message for all telegram bot users by part 800messages
it creates send records from your users collection

Install
=======

`npm install tgsend`

or
`yarn add tgsend`

Examples
========

```javascript
const tgsend = require('tgsend');

// your send function
processSend(ctx) {
    // conn with users collection
    this.conn = createConnection(process.env.MONGO_URI_USERS);
    // for broadcast collection
    this.connSend = createConnection(process.env.MONGO_URI_NEW);
    tgsend(ctx, this);
}

```

implemented https://github.com/albertincx/formatbot1

IMPORTANT 

you should implement getConf, toggleConfig, isAdmin, getMidMessage, forwardMes, sendAdmin methods


## TODO

- create distribution records in mongo
- start distribution
- ~~cron~~
- ~~tests~~

# npm link

https://www.npmjs.com/package/tgsend
