package com.wenthomas.bank

import scala.beans.BeanProperty

/**
 * @author Verno
 * @create 2020-03-05 21:52 
 */
/**
 * 扩展如下的 BankAccount类，新类 CheckingAccount 对每次存款和取款都收取1美元的手续费
 */
object Bank {
    def main(args: Array[String]): Unit = {
        val account = new CheckingAccount(1000)
        println(account.withdraw(100))
        println(account.deposit(10))
    }
}

class BankAccount(initialBalance: Double) {
    private var balance = initialBalance

    def deposit(amount: Double) = {
        balance += amount
        balance
    }

    def withdraw(amount: Double) = {
        balance -= amount
        balance
    }
}

class CheckingAccount(initialBalance: Double) extends BankAccount(initialBalance) {

    override def deposit(amount: Double): Double = {
        super.deposit(amount - 1)
    }

    override def withdraw(amount: Double): Double = {
        super.withdraw(amount + 1)
    }
}