var source = new EventSource('/stream');

var pdfData = [];
var MAX_WORDS_PER_FILE = 10;



// example object
var dummyDataStruct = [
  {
  "title" : "pdfTitle",
  "words" : [
    "word1",  "word2"
  ],
  "counts" : [
    10, 5
  ],
  "minCount" : 0
  }
];


/*
function isBookStored(pdfTitle){
  for(var index = 0; index < pdfData.length ; index++){
    if(pdfData[index]['title'] === pdfTitle){
      return true;
    }
  }
  return false;
}
*/






function computeNewMin(bookContent){
  return Array.min(bookContent['counts']);
}

Array.min = function( array ){
    return Math.min.apply( Math, array );
};

/*

IDEAL PROGRAM FLOW:




3- is the word stored?
3.1 - iterate and store if not
3.2 - update and reorder
4 - update min value
5 - remove extra word if any

*/

source.onmessage = function (event) {
  // check if valid tuple
  if(!isValidTuple(event))
    return;

  // parse the incoming tuple
  pdfTitle = getPdfTitle(event);
  word = getWord(event);
  count = getCount(event);

  // retrieve pdf index
  var pdfIndex = retrievePdfIndex(pdfTitle);

  // initialize new books && retrieve new index
  if(pdfIndex === -1){
    pdfData.push(initializePdf(pdfTitle));
    pdfIndex = retrievePdfIndex(pdfTitle);
  }

  if(!isWordStored(pdfIndex, word)){
    // word count big enough
    if(isNewWordValid(pdfIndex, word, count)){
      addWord(pdfIndex, word, count);
    }
  } else {
    // update values
    var wordIndex = getWordIndex(pdfIndex, word);
    updateWordValues(pdfIndex, wordIndex, count);
    // position
    //relocateWord(pdfIndex, wordIndex);
  }

  removeExtraWords(pdfIndex);

  updateMinCount(pdfIndex);

};


//update display every #1000 milliseconds
window.setInterval(updateBookInformation, 250);
