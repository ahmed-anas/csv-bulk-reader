const expect = require('chai').expect;
const CsvBulkReader = require('../index').CsvBulkReader;


/**
 * 
 */
let reader = null;

describe('csv read one by one', () => {
    it('should initialize', function () {
        reader = new CsvBulkReader('./test/test-files/testdata.csv', { readLinesSize: 2 });
    });
    it('should read first row', async function () {
        let row = await reader.getRow();
        expect(`{"domain":"abc.com","description":"it's a good domain"}`).to.equal(JSON.stringify(row));
        return row;
    });

    it('should read last row and all rows', async function () {

        let count = 0;

        let row = null;

        while (v = await reader.getRow()) {
            count++;
            row = v;
        }
        expect(count).to.equal(27);
        expect(`{"domain":"ddd.com","description":"asdf"}`).to.equal(JSON.stringify(row));
        return row;
    });

})


describe('csv read chunk', () => {
    it('should read full chunk', async () => {
        reader = new CsvBulkReader('./test/test-files/testdata.csv', { readLinesSize: 500 });

        let all = [];
        let chunk;
        while(chunk = await reader.getChunk()){
            all.push(...chunk);
        }
        expect(all.length).to.equal(28);
        return;
    })
})