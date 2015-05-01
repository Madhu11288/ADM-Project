var redisHashTags = new EventSource('/redis-hashtags-stream');
var aerospikeHashTags = new EventSource('/aerospike-hashtags-stream');

var maxSize = 100;

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
                var size = maxSize > pixel ? pixel : maxSize;
                return  size + "px";
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
                var size = maxSize > pixel ? pixel : maxSize;
                return  size + "px";
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
