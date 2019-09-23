const csv_parser = require('csv-parser');
const fs = require('fs');
const _ = require('lodash');
class CsvBulkReader {
    /**
     * 
     * @param {*} inputFilePath 
     * @param {{readLinesSize: number}} config 
     */
    constructor(inputFilePath, config) {
        this.inputFilePath = inputFilePath;
        this.config = config;
        this.dataBuffer = [];
        this.hasEnded = false;
        this.error = null;

        this.getRowPromise = null;
        this.getChunkPromise = null;

        this.config = _.extend(this.config, {
            readLinesSize: 500
        });


        this.inputStream = fs.createReadStream(inputFilePath).pipe((csv_parser()));


        this.inputStream.on('data', async (row) => {

            //remove
            // setTimeout(()=>{
            this.dataBuffer.push(row);
            if (this.dataBuffer.length >= this.config.readLinesSize) {
                this._resolvePromises();
                this.inputStream.pause();
            }
            // },4000)

        }).on('end', (code) => {
            this._resolvePromises();
            this.hasEnded = true;
        }).on('error', (error) => {
            this.hasEnded = true;
            this._rejectPromises();
            this.error = error || 'Unknown Error';
        })
    }


    _resolvePromises(value) {
        [this.getChunkPromise, this.getRowPromise].forEach(v => {
            v && v.resolve(value);
        })
    }
    _rejectPromises(err) {
        [this.getChunkPromise, this.getRowPromise].forEach(v => {
            v && v.reject(err);
        })
    }

    _checkError() {
        if (this.error) {
            throw new Error(error);
        }

    }

    _createPromiseObj() {
        let promiseObj = {};
        let promise = new Promise((resolve, reject) => {
            promiseObj.resolve = resolve;
            promiseObj.reject = reject;
        });
        promiseObj.promise = promise;
        return promiseObj;
    }

    getChunk() {
        this._checkError();
        if (!this.dataBuffer.length && this.hasEnded) { return null };
        if (this.dataBuffer.length) {
            let v = this.dataBuffer;
            this.dataBuffer = [];
            return Promise.resolve(v);
        }
        if (!this.getChunkPromise) {
            this.getChunkPromise = this._createPromiseObj();
        }

        this.inputStream.resume();
        return this.getChunkPromise.promise.then(v => {
            return this.getChunk();
        });


    }

    getRow() {
        this._checkError();
        if (!this.dataBuffer.length && this.hasEnded) { return null };
        if (this.dataBuffer.length) {
            return Promise.resolve(this.dataBuffer.shift());
        }
        if (!this.getRowPromise) {
            this.getRowPromise = this._createPromiseObj();
        }

        this.inputStream.resume();
        return this.getRowPromise.promise.then(v => {
            return this.getRow();
        });
    }

}


module.exports.CsvBulkReader = CsvBulkReader;