var redisHashTags = new EventSource('/redis-hashtags-stream');
var aerospikeHashTags = new EventSource('/aerospike-hashtags-stream');
//var tweets = new EventSource('/tweets-stream');


redisHashTags.onmessage = function (event) {
    var fill = d3.scale.category20();
    var topics = event.data;

    var list = [];
    var topicsList = topics.split("|%*%|");
    for (var i = 0; i < topicsList.length; i++) {
        var key = topicsList[i].split("|")[0];
        var count = topicsList[i].split("|")[1];
        var trending_topics = {};
        trending_topics["text"] = key;
        trending_topics["size"] = count;
        list.push(trending_topics)
    }

    $("#redis-hash-svg").empty();

    d3.layout.cloud().size([600, 500])
        .words(list)
        .rotate(0)
        .fontSize(function (d) {
            return d.size;
        })
        .on("end", draw)
        .start();

    function draw(words) {
        d3.select("#redis-hash-svg")
            .append("g")
            .attr("transform", "translate(280, 300)")
            .selectAll("text")
            .data(words)
            .enter().append("text")
            .style("font-size", function (d) {
                var pixel = (600 * d.size) / 500;
                return  pixel + "px";
            })
            .style("font-family", "Impact")
            .style("fill", function (d, i) {
                return fill(i);
            })
            .attr("text-anchor", "middle")
            .attr("transform", function (d) {
                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
            })
            .text(function (d) {
                return d.text;
            });
    }
};

aerospikeHashTags.onmessage = function (event) {
    var fill = d3.scale.category20();
    var topics = event.data;

    var list = [];
    var topicsList = topics.split("|%*%|");
    for (var i = 0; i < topicsList.length; i++) {
        var key = topicsList[i].split("|")[0];
        var count = topicsList[i].split("|")[1];
        var trending_topics = {};
        trending_topics["text"] = key;
        trending_topics["size"] = count;
        list.push(trending_topics)
    }

    $("#aerospike-hash-svg").empty();

    d3.layout.cloud().size([600, 500])
        .words(list)
        .rotate(0)
        .fontSize(function (d) {
            return d.size;
        })
        .on("end", draw)
        .start();

    function draw(words) {
        d3.select("#aerospike-hash-svg")
            .append("g")
            .attr("transform", "translate(280, 300)")
            .selectAll("text")
            .data(words)
            .enter().append("text")
            .style("font-size", function (d) {
                var pixel = (600 * d.size) / 500;
                return  pixel + "px";
            })
            .style("font-family", "Impact")
            .style("fill", function (d, i) {
                return fill(i);
            })
            .attr("text-anchor", "middle")
            .attr("transform", function (d) {
                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
            })
            .text(function (d) {
                return d.text;
            });
    }
};


//tweets.onmessage = function (event) {
//    var tweetsData = event.data;
////
//    var list = [];
//    var tweets = tweetsData.split("|%*%|");
//    var tweetsSelector = $("#tweets");
//    tweetsSelector.empty();
//    for (var i = 0; i < tweets.length; i++) {
//        var tweetText = tweets[i];
//        var tweet = {};
//        tweet["name"] = tweetText;
//        list.push(tweet);
//    }
////
//    console.log(list.length);
//    $("#tweets-svg").empty();
//    var scrollSVG = d3.select("#tweets-svg")
//        .attr("class", "scroll-svg");
////
//    var chartGroup = scrollSVG.append("g")
//        .attr("class", "chartGroup");
//    chartGroup.append("rect")
//        .attr("fill", "#FFFFFF");
////
//    var rowEnter = function (rowSelection) {
//        rowSelection.append("rect")
//            .attr("rx", 3)
//            .attr("ry", 3)
//            .attr("width", "600")
//            .attr("height", "24")
//            .attr("fill-opacity", 0.25)
//            .attr("stroke", "#999999")
//            .attr("stroke-width", "2px");
//        rowSelection.append("text")
//            .attr("transform", "translate(10,15)");
//    };
////
//    var rowUpdate = function (rowSelection) {
//        rowSelection.select("rect");
//        rowSelection.select("text")
//            .text(function (d) {
//                return d.name;
//            });
//    };
////
//    var rowExit = function (rowSelection) {
//    };
////
//    var virtualScroller = d3.VirtualScroller()
//        .rowHeight(30)
//        .enter(rowEnter)
//        .update(rowUpdate)
//        .exit(rowExit)
//        .svg(scrollSVG)
//        .totalRows(50)
//        .viewport(d3.select(".viewport"));
////
//    virtualScroller.data(list, function (d) {
//        return d.name;
//    });
////
//    chartGroup.call(virtualScroller);
//};
////

