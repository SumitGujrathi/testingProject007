const { spawn } = require("child_process");
const express = require("express");
// const spawn = require("child_process").spawn;


const app = express();


app.get("/python", callName);

app.get("/test", test);

app.get("/", function(req,res){
    res.send("Hi there this is testing check...!!!")
});


function callName(req, res){
    // var spawn = require("child_process").spawn;

    var process = spawn("python", ["./main.py"])

    process.stdout.on("data", function (data){
        res.send(data.toString());
    });
};

function test(req, res){
    // var spawn = require("child_process").spawn;

    var process = spawn("python", ["./test.py"])

     process.stdout.on("data", function (data){
         res.send(data.toString());
    });
};


app.listen("mongodb+srv://sumitgujrathi24:8lpVTDIBtyHKWRH3@sumit.ybjewjm.mongodb.net/NSE?retryWrites=true&w=majority", function(){
    console.log("running on")
})


