package mapReduceTasks;

public class Main {

	public static void main(String[] args) {
		if(args.length==0 || args.length > 2){
			System.out.println("use commad 'CreateQuadTreeIndex' or 'Query'");
			return;
		}
		if(args[0] == "CreateQuadTreeIndex"){
			QuadTreeIndexer.main(args);
		} else {
			QuadTreeQuery.main(args);
		}
		
	}

}
