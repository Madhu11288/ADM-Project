var trendingHastags = new EventSource('/trending-hashtags-stream');
var tweets = new EventSource('/tweets-stream');
var hash = {};
var width = 1200;
var height = 700;

//update hash (associative array) with incoming word and count
trendingHastags.onmessage = function (event) {
  console.log(event);
};

tweets.onmessage = function (event) {
  console.log(event);
};

//window.setTimeout(function(){location.reload()}, 3000);
