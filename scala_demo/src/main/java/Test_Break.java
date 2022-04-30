/**
 * @author Adam-Ma
 * @date 2022/4/30 19:37
 * @Project BigData_Review_Adam
 * @email Adam_Ma520@outlook.com
 * @phone 18852895353
 */

/**
 *     使用 Java 中跳出循环：
 *          1、break
 *          2、抛出异常的方式
 */
public class Test_Break {
    public static void main(String[] args) {
        // 1、break 的方式
        for (int i = 0; i < 10; i++) {
            if (i == 5) {
                break;
            }
            System.out.println(i);
        }
        System.out.println("循环外的代码");

        System.out.println("===============================");
        try {
            for (int i = 0; i < 10; i++) {
                if (i == 5) {
                    throw new Exception("抛出异常");
                }
                System.out.println(i);
            }
        } catch (Exception e) {
        }
        System.out.println("循环外的代码");
    }
}
