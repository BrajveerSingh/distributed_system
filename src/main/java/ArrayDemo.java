import java.util.Arrays;

public class ArrayDemo {
    public static void main(String[] args) {
        //Given the array arr[] consisting of 2n elements in the form [x1,x2,...,xn,y1,y2,...,yn]. Return the array in the form [x1,y1,x2,y2,...,xn,yn]
        //Input: arr = [2,5,1,3,4,7], n = 3
        //Output: [2,3,5,4,1,7]
        int[] arr = {2, 5, 1, 3, 4, 7};//
        printXAndYArray(arr);
    }

    private static void printXAndYArray(final int[] arr) {
        if(arr.length %2 != 0) {
            throw new IllegalArgumentException("Array length should be even");
        }
        int i = 0;
        int length = arr.length;
        int j = length/2;
        int[] result = new int[length];
        int index = 0;

        while(i < length/2 && j < length) {
            result[index] = arr[i];    //0->2     5   1
            result[index+1] = arr[j]; //1->3      4   7
            i++;
            j++;
            index++;
        }

        System.out.println("arr = " + Arrays.toString(result));
    }
}
