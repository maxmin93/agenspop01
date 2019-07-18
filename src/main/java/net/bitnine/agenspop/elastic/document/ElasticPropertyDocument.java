package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticProperty;

// **NOTE : 전체 vertex, edge 대상으로 검색 등을 하려면
//      대상 전체가 메모리에 올라와야 하는데, 이럴려면 values 등은 분리하는게
//      성능상에서 이점이 있지 않을까? (검색 속도, 검색 용량 증가)
//      -- 단점 : value 찾을 때, 한번 더 es-index 에 대해 call 한다는 것
//          1) 출력할 때, n개 element 만큼 n번 property index 호출
//          2) 예를 들어 age > 30 이상의 element 검색시 최적화 필수
//              , 안그러면 element 들 찾고 property 들 찾고 한번 더 element 필터링
//          3) 데이터 임포트 시에 두군데 es index 로 나눠 보내던가, 2회 돌리던가
//
//      ==> 풀스캔 처리가 목표가 아닌데, 굳이 이럴 필요가 있나? 일단 보류!
//

public class ElasticPropertyDocument implements ElasticProperty {

    // **NOTE: private 설정시 SerializationFeature.FAIL_ON_EMPTY_BEANS
    // private String elementId;      // ID ==> datasource + "::" + eid
    private String key;
    private String type;
    private String value;

    public ElasticPropertyDocument(){}
    public ElasticPropertyDocument(final String key, final String type, final Object value) {
        // this.elementId = elementId;
        this.key = key;
        this.type = type;
        this.value = value.toString();
    }

    // @Override public String elementId() { return this.elementId; }
    @Override public String getKey() { return this.key; }
    @Override public String getType() { return this.type; }
    @Override public String getValue() { return this.value; }

    @Override
    public String toString() {
        return String.format("%s<%s>=%s", key, type, value);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (this.getClass() != obj.getClass()) return false;
        if (this == obj) return true;

        ElasticPropertyDocument that = (ElasticPropertyDocument) obj;
        if (this.key == null || that.getKey() == null || !this.key.equals(that.getKey()) )
            return false;
        if (this.value == null || that.getValue() == null || !this.value.equals(that.getValue()))
            return false;
        if (this.type == null || that.getType() == null || !this.type.equals(that.getType()) )
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return 31*value.hashCode() + 43*type.hashCode() + 59*key.hashCode();
    }

}
