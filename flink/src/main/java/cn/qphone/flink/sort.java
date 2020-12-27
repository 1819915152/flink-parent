package cn.qphone.flink;

public class sort {
    public static void main(String[] args) {
        //[1,8,4,9,234,78]
        //System.out.println(returnIndexNum(1));
     returnIndexNum(1);
    }
    public static void returnIndexNum(int k){
        int []arr = new int[]{1,8,4,9,234,78};
        //
        printArr(arr);
        for(int i=1;i< arr.length;i++){
            int max=i;
            for(int j=i;j<arr.length;j++){
                if(arr[j-1]<arr[j]){
                    max = j-1;
                }
            }
            if(max!=i){
                swap(arr,max,i);
            }
        }
        printArr(arr);
        //return arr[k];
    }
    public static void swap(int[] arr,int max,int i){
        int temp =arr[max];
        arr[max] = arr[i];
        arr[i]=temp;
    }
    public static void printArr(int[] arr){
        for(int i=0;i<arr.length;i++){
            System.out.print(arr[i]+"\t");
        }
        System.out.println();
    }
}
