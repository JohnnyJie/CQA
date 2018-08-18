package model;

public enum Sign {
	equal("="), greaterORequal(">="), greater(">"), lessORequal("<="), less("<"), notequal("<>");
	public String enumField;

	private Sign(String enumField) {
		this.enumField = enumField;
	}
}
