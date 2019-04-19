package com.kelv1n;

public class Bytes {

    /**
     * Get a long number from a bytes array starting from the provided {@code index}.
     *
     * @param memory the bytes array
     * @param index  the starting index
     * @return the long number.
     */
    public static long toLong(byte[] memory, int index) {
        return ((long) memory[index] & 0xff) << 56
                | ((long) memory[index + 1] & 0xff) << 48
                | ((long) memory[index + 2] & 0xff) << 40
                | ((long) memory[index + 3] & 0xff) << 32
                | ((long) memory[index + 4] & 0xff) << 24
                | ((long) memory[index + 5] & 0xff) << 16
                | ((long) memory[index + 6] & 0xff) << 8
                | (long) memory[index + 7] & 0xff;
    }

    /**
     * Convert a long number to a bytes array.
     *
     * @param value the long number
     * @return the bytes array
     */
    public static byte[] toBytes(long value) {
        byte[] memory = new byte[8];
        toBytes(value, memory, 0);
        return memory;
    }

    public static void toBytes(long value, byte[] memory, int index) {
        memory[index] = (byte) (value >>> 56);
        memory[index + 1] = (byte) (value >>> 48);
        memory[index + 2] = (byte) (value >>> 40);
        memory[index + 3] = (byte) (value >>> 32);
        memory[index + 4] = (byte) (value >>> 24);
        memory[index + 5] = (byte) (value >>> 16);
        memory[index + 6] = (byte) (value >>> 8);
        memory[index + 7] = (byte) value;
    }

    /**
     * Get a int number from a bytes array starting from the provided {@code index}.
     *
     * @param memory the bytes array
     * @param index  the starting index
     * @return the long number.
     */
    public static int toInt(byte[] memory, int index) {
        return (memory[index] & 0xff) << 24
                | (memory[index + 1] & 0xff) << 16
                | (memory[index + 2] & 0xff) << 8
                | memory[index + 3] & 0xff;
    }

    /**
     * Convert a int number to a bytes array.
     *
     * @param value the int number
     * @return the bytes array
     */
    public static byte[] toBytes(int value) {
        byte[] memory = new byte[4];
        toBytes(value, memory, 0);
        return memory;
    }

    /**
     * Big Endian transfer.
     * @param value
     * @param memory
     * @param index
     */
    public static void toBytes(int value, byte[] memory, int index) {
        memory[index] = (byte) (value >>> 24);
        memory[index + 1] = (byte) (value >>> 16);
        memory[index + 2] = (byte) (value >>> 8);
        memory[index + 3] = (byte) value;
    }

}
