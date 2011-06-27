package cn.ict.cacuts.mapreduce.compiler;


public final class ParallelLevel {
	private static final int LevelFirst = 0;
	private static final int LevelEnd = -1;
	private static final int LevelMiddleFactor = 1;
	private  int level ;
	public ParallelLevel() {
		this.level  = 0;
	}
	public ParallelLevel(int level) {
		this.level = level;
	}
	public ParallelLevel(ParallelLevel plevel) {
		this.level = plevel.getLevel();
	}
	protected int getLevel() {
		return level;
	}
	
	public static ParallelLevel assignFirstLevel() {
		return new ParallelLevel(LevelFirst);
	}

	public static ParallelLevel assignEndLevel() {
		return new ParallelLevel(LevelEnd);
	}
	public  ParallelLevel currentLevel() {
		return  new ParallelLevel(getLevel());
	}	

	public ParallelLevel nextLevel() {
		return new ParallelLevel(level + 1); 
	}
	
	
}
