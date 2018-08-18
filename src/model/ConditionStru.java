package model;

public class ConditionStru {
	private String leftTerm;
	private String rightTerm;
	private Sign sign;
	private String symbel;

	public ConditionStru(String leftTerm, String rightTerm, String signStr, Boolean isNot) throws Exception {
		this.leftTerm = leftTerm;
		this.rightTerm = rightTerm;

		switch (signStr) {
		case "=":
			if (!isNot)
				this.sign = Sign.equal;
			else
				this.sign = Sign.notequal;
			break;
		case "<=":
			if (!isNot)
				this.sign = Sign.lessORequal;
			else
				this.sign = Sign.greater;
			break;
		case "<":
			if (!isNot)
				this.sign = Sign.less;
			else
				this.sign = Sign.greaterORequal;
			break;
		case ">=":
			if (!isNot)
				this.sign = Sign.greaterORequal;
			else
				this.sign = Sign.less;
			break;
		case ">":
			if (!isNot)
				this.sign = Sign.greater;
			else
				this.sign = Sign.lessORequal;
			break;
		case "<>":
			if (!isNot)
				this.sign = Sign.notequal;
			else
				this.sign = Sign.equal;
			break;
		default:
			throw new Exception("constraint format error : sign error");
		}
		this.symbel = setSign(this.sign);
	}

	public String setSign(Sign sign) {
		switch (sign) {
		case equal:
			return "=";
		case greaterORequal:
			return ">=";
		case greater:
			return ">";
		case lessORequal:
			return "<=";
		case less:
			return "<";
		case notequal:
			return "<>";
		default:
			return "";
		}
	}

	public String getLeftTerm() {
		return leftTerm;
	}

	public String getRightTerm() {
		return rightTerm;
	}

	public Sign getSign() {
		return sign;
	}

	public String getSymbel() {
		return symbel;
	}
}