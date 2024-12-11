const co = require('co');

const cBroad = '/createBroadcast';
const sBroad = '/startBroadcast';

const logger = (r) => console.log(r);

const processRows = async (cc, limit = 25, timeout, cb) => {
    if (!cb) return;

    let items = [];
    await co(function* () {
        for (let doc = yield cc.next(); doc != null; doc = yield cc.next()) {
            const item = doc.toObject();
            if (items.length === limit) {
                try {
                    yield cb(items);
                } catch (e) {
                    console.log(e);
                }
                items = [];
                if (timeout) {
                    yield new Promise(resolve => setTimeout(() => resolve(), timeout));
                }
            }
            items.push(item);
        }
    });

    if (items.length) {
        try {
            await cb(items);
        } catch (e) {
            console.log(e);
        }
    }
};

const getCmdParams = txt => {
    let params = txt.match(/r_c_id_([0-9_-]+)/);
    if (params && params[1]) {
        params = params[1].split('_');
        params = params.map(Number);
    }

    return params || [];
};

const createBroadcast = async (ctx, txt, botHelper) => {
    const [cId] = getCmdParams(txt);
    if (!cId) return ctx.reply('broad err no id');

    const connSecond = botHelper.conn;

    const messages = connSecond.model('broadcasts', botHelper.schema);

    const users = connSecond.model('users', botHelper.schema);
    let filter = {};

    const cursor = users.find(filter).cursor();

    let updates = [], document;

    while ((document = await cursor.next())) {
        const {id} = document.toObject();
        updates.push({
            insertOne: {
                document: {
                    uid: id,
                    cId
                }
            },
        });

        if (updates.length % 1000 === 0) {
            console.log(`updates added ${updates.length}`);
            await messages.bulkWrite(updates, {ordered: false}).catch(e => console.log(e));
            updates = [];
        }
    }

    if (updates.length) {
        console.log(`updates added ${updates.length}`);
        await messages.bulkWrite(updates, {ordered: false}).catch(e => console.log(e));
    }

    const updFilter = {
        cId,
        sent: {$exists: false}
    };
    const cnt = await messages.countDocuments(updFilter);
    ctx.reply(`broad ${cId} created: ${cnt}`);

    return connSecond.close();
};

const startBroadcast = async (ctx, txtParam, botHelper) => {
    const [cId, mId, fromId, isChannel] = getCmdParams(txtParam);
    if (!cId) {
        return ctx.reply('broad err no id');
    }
    let preMessage = botHelper.getMidMessage(mId);
    const result = {
        err: 0,
        success: 0,
    };

    const connSend = botHelper.connSend;

    const messages = connSend.model('broadcasts', botHelper.schema);

    const filter = {
        sent: {$exists: false},
        cId,
    };

    const cursor = messages.find(filter)
        .limit(800)
        .cursor();

    let breakProcess = false;

    await processRows(cursor, 5, 500, async items => {
        if (breakProcess) {
            return;
        }
        const success = [];
        try {
            for (let itemIdx = 0; itemIdx < items.length; itemIdx += 1) {
                if (breakProcess) break;

                const {
                    _id,
                    uid: id
                } = items[itemIdx];

                const runCmd = () => botHelper.forwardMes(mId, fromId * (isChannel ? -1 : 1), id);
                const preCmd = !preMessage ? false : (() => botHelper.sendAdmin(preMessage, id));

                try {
                    if (preCmd) {
                        logger('run preCmd');
                        await preCmd();
                    }
                    logger('runCmd');
                    await runCmd();

                    success.push({
                        updateOne: {
                            filter: {_id},
                            update: {sent: true},
                        },
                    });
                    result.success += 1;
                } catch (e) {
                    logger(e);
                    if (e.code !== 'ETIMEDOUT') {
                        if (e.code === 429) {
                            breakProcess = JSON.stringify(e);
                        }
                        result.err += 1;
                        success.push({
                            updateOne: {
                                filter: {_id},
                                update: {
                                    sent: true,
                                    error: JSON.stringify(e),
                                    code: e.code,
                                },
                            },
                        });
                    }
                }
            }
        } catch (e) {
            logger(e);
            if (e.code === 429) {
                if (e.response.parameters) {
                    // logger(e.response.parameters.retry_after);
                }
                breakProcess = JSON.stringify(e);
            }
        }
        if (success.length) {
            await messages.bulkWrite(success);
        }
    });

    const resulStr = `${JSON.stringify(result)}`;
    const cntSent = await messages.countDocuments({
        cId,
        sent: true
    });
    const cntTotal = await messages.countDocuments({cId});

    let log = `${cntTotal}/${cntSent}`;

    if (cntTotal && cntTotal === cntSent) {
        const cntActive = await messages.countDocuments({
            cId,
            error: {$exists: false}
        });

        log += `/${cntActive}`;
        botHelper.toggleConfig({
            text: 'broadcast',
            chat: ctx.message.chat
        }, false);
    }
    await connSend.close();

    try {
        return ctx.reply(`broad completed: ${resulStr} with ${breakProcess || ''} ${log}`);
    } catch (e) {
        // logger(e);
    }
};

const processBroadcast = async (txtParam, ctx, botHelper) => {
    let txt = txtParam;
    if (txt.match(cBroad)) {
        ctx.reply('broad new started');
        return createBroadcast(ctx, txt, botHelper);
    }
    if (txt.match(sBroad)) {
        txt = txt.replace(sBroad, '');
        ctx.reply('broad send started');
        await startBroadcast(ctx, txt, botHelper);
    }
    return Promise.resolve();
};

const tgsend = (ctx, botHelper) => {
    const {
        chat: {id: chatId},
        text,
    } = ctx.message;
    if (!botHelper.isAdmin(chatId) || !text) {
        return Promise.resolve(true);
    }

    processBroadcast(text, ctx, botHelper);
};

module.exports = tgsend;
