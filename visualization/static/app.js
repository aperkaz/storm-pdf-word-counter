var source = new EventSource('/stream');

var pdfData = [];
var MAX_WORDS_PER_FILE = 10;

function computeNewMin(bookContent){
  return Array.min(bookContent['counts']);
}

Array.min = function( array ){
    return Math.min.apply( Math, array );
};

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
    reorderWord(pdfIndex, wordIndex);
  }

  removeExtraWords(pdfIndex);
  updateMinCount(pdfIndex);

};


//update display every #1000 milliseconds
window.setInterval(updateBookInformation, 250);
