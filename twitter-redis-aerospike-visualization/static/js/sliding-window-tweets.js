var redisTweets = new EventSource('/redis-tweets-stream');

redisTweets.onmessage = function (event) {
    var tweetsData = event.data;

    var list = [];
    var tweets = tweetsData.split("|%*%|");
    var tweetsSelector = $("#redis-tweets");
    tweetsSelector.empty();
    for (var i = 0; i < tweets.length; i++) {
        var tweetText = tweets[i];
        var tweet = {};
        tweet["name"] = tweetText;
        list.push(tweet);
    }

    $("#redis-tweets-svg").empty();
    var scrollSVG = d3.select("#redis-tweets-svg")
        .attr("class", "scroll-svg");

    var chartGroup = scrollSVG.append("g")
        .attr("class", "chartGroup");
    chartGroup.append("rect")
        .attr("fill", "#FFFFFF");

    var rowEnter = function (rowSelection) {
        rowSelection.append("rect")
            .attr("rx", 3)
            .attr("ry", 3)
            .attr("width", "600")
            .attr("height", "24")
            .attr("fill-opacity", 0.25)
            .attr("stroke", "#999999")
            .attr("stroke-width", "2px");
        rowSelection.append("text")
            .attr("transform", "translate(10,15)");
    };

    var rowUpdate = function (rowSelection) {
        rowSelection.select("rect");
        rowSelection.select("text")
            .text(function (d) {
                return d.name;
            });
    };

    var rowExit = function (rowSelection) {
    };

    var virtualScroller = d3.VirtualScroller()
        .rowHeight(30)
        .enter(rowEnter)
        .update(rowUpdate)
        .exit(rowExit)
        .svg(scrollSVG)
        .totalRows(list.length)
        .viewport(d3.select(".redis-viewport"));

    virtualScroller.data(list, function (d) {
        return d.name;
    });

    chartGroup.call(virtualScroller);
};


var aerospikeTweets = new EventSource('/aerospike-tweets-stream');

aerospikeTweets.onmessage = function (event) {
    var tweetsData = event.data;

    var list = [];
    var tweets = tweetsData.split("|%*%|");
    var tweetsSelector = $("#aerospike-tweets");
    tweetsSelector.empty();
    for (var i = 0; i < tweets.length; i++) {
        var tweetText = tweets[i];
        var tweet = {};
        tweet["name"] = tweetText;
        list.push(tweet);
    }

    $("#aerospike-tweets-svg").empty();
    var scrollSVG = d3.select("#aerospike-tweets-svg")
        .attr("class", "scroll-svg");

    var chartGroup = scrollSVG.append("g")
        .attr("class", "chartGroup");
    chartGroup.append("rect")
        .attr("fill", "#FFFFFF");

    var rowEnter = function (rowSelection) {
        rowSelection.append("rect")
            .attr("rx", 3)
            .attr("ry", 3)
            .attr("width", "600")
            .attr("height", "24")
            .attr("fill-opacity", 0.25)
            .attr("stroke", "#999999")
            .attr("stroke-width", "2px");
        rowSelection.append("text")
            .attr("transform", "translate(10,15)");
    };

    var rowUpdate = function (rowSelection) {
        rowSelection.select("rect");
        rowSelection.select("text")
            .text(function (d) {
                return d.name;
            });
    };

    var rowExit = function (rowSelection) {
    };

    var virtualScroller = d3.VirtualScroller()
        .rowHeight(30)
        .enter(rowEnter)
        .update(rowUpdate)
        .exit(rowExit)
        .svg(scrollSVG)
        .totalRows(list.length)
        .viewport(d3.select(".aerospike-viewport"));

    virtualScroller.data(list, function (d) {
        return d.name;
    });

    chartGroup.call(virtualScroller);
};

