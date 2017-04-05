package storm.utils;

public class Word {

	private String content;
	private Long count;

	public Word(String content, Long count){
		this.content = content;
		this.count = count;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

}
