 function log(msg){
	 if(typeof(vjo) != "undefined"){
		 vjo.sysout.println(msg);
	 }else{
		 console.log(msg);
	 }
	 
 }
 
 function TopList(size,comparator){
	 this.size=size;
	 this.cnt=0;
	 this.root=null;
	 this.tail=null;
	 this.comparator=comparator;
 }
 
 TopList.Node = function(val){
	 this.prev=null;
	 this.next=null;
	 this.data=val;
 }
 
 TopList.prototype.clear = function(){
	 this.root=null;
	 this.tail=null;
	 this.cnt=0;
 }
 
 
 TopList.prototype.add = function(item){
	 if(this.root==null){
		 this.root=new TopList.Node(item);
		 this.tail=this.root;
		 this.cnt=1;
	 }
	 else{
		 var node=new TopList.Node(item);
		 
		 if(this.cnt == this.size && this.comparator(this.tail.data,item)<0){
			 return;  //short circuit (too small)
		 }
		 
		 //find position (insertion sort)
		 var curr=this.root;
		 var added=false;
		 while(curr!=null){
			 if(this.comparator(curr.data,node.data)>0){ //insert
				 added=true;
				 this.cnt++;
				 node.prev=curr.prev;
				 if(node.prev!=null){
					 node.prev.next=node;
				 }else{
					 this.root=node;
				}
				node.next=curr;
				curr.prev=node;
				break;
			 }
			 else{
			 }
			 curr=curr.next;
		 }
		 if(!added){ //append
			 this.tail.next=node;
			 node.prev=this.tail;
			 this.tail=node;
			 this.cnt++;
		 }
		 
		 if(this.cnt > this.size){
			 this.tail=this.tail.prev;
			 this.tail.next=null;
			 this.cnt--;
		 }
	 }
 };
 
