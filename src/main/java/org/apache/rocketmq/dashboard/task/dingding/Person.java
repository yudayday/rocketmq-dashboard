package org.apache.rocketmq.dashboard.task.dingding;

/**
 * @author zero
 */
@SuppressWarnings("all")
public enum Person {

    YU_TIANTIAN("俞天天", "15355814054"),
    SHI_PENG("石鹏", "17775632084");

    private String name;

    private String telephone;

    private Person(String name, String telephone) {
        this.name = name;
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }
}
