const fs = require('fs')

let csv = '';

for (let i = 0; i < 100000; i++) {
    csv += Math.floor(Math.random() * Math.floor(10));
    csv += '\n'
}
fs.writeFile('./data.csv', csv, err => {
    if (err) {
        console.log(err)
    } else {
        console.log("DONE")
    }
})